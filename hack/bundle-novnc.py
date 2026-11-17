#!/usr/bin/env python3
"""Bundle noVNC's npm package (CommonJS, `lib/`) into one browser script
that sets `window.RFB`, for the dashboard's VM console.

    curl -fsSLO https://registry.npmjs.org/@novnc/novnc/-/novnc-1.5.0.tgz
    tar xzf novnc-1.5.0.tgz
    python3 -I hack/bundle-novnc.py package web/novnc

Every `require` in the package is relative (some climb out of lib/ and back
in), so modules are keyed by their path in the package and a tiny module
table is all the loader there is. The version is in the output's name: the dashboard
serves its assets as immutable.
"""

import json
import os
import shutil
import sys


def main(pkg, out_dir):
    meta = json.load(open(os.path.join(pkg, "package.json")))
    version = meta["version"]
    lib = os.path.join(pkg, "lib")
    modules = {}
    for root, _, files in os.walk(lib):
        for f in sorted(files):
            if f.endswith(".js"):
                path = os.path.join(root, f)
                rel = os.path.relpath(path, pkg).replace(os.sep, "/")
                modules[rel] = open(path, encoding="utf-8").read()
    if "lib/rfb.js" not in modules:
        sys.exit("no lib/rfb.js in %s" % pkg)

    parts = [
        "/* noVNC %s (https://github.com/novnc/noVNC), MPL-2.0: see LICENSE.txt.\n"
        "   Bundled from the npm package by hack/bundle-novnc.py. */\n" % version,
        "(function () {\n'use strict';\nvar defs = {};\n",
    ]
    for rel in sorted(modules):
        parts.append(
            "defs[%s] = function (module, exports, require) {\n%s\n};\n"
            % (json.dumps(rel), modules[rel])
        )
    parts.append(
        """var cache = {};
function resolve(from, to) {
  var base = from.split('/'); base.pop();
  to.split('/').forEach(function (p) {
    if (p === '..') base.pop(); else if (p !== '.' && p !== '') base.push(p);
  });
  var id = base.join('/');
  if (!defs[id] && defs[id + '.js']) id += '.js';
  if (!defs[id] && defs[id + '/index.js']) id += '/index.js';
  return id;
}
function load(id) {
  if (cache[id]) return cache[id].exports;
  if (!defs[id]) throw new Error('novnc bundle: no module ' + id);
  var module = { exports: {} };
  cache[id] = module;
  defs[id].call(module.exports, module, module.exports, function (to) {
    return load(resolve(id, to));
  });
  return module.exports;
}
window.RFB = load('lib/rfb.js')['default'];
})();
"""
    )
    os.makedirs(out_dir, exist_ok=True)
    out = os.path.join(out_dir, "novnc-%s.js" % version)
    with open(out, "w", encoding="utf-8") as f:
        f.write("".join(parts))
    shutil.copy(os.path.join(pkg, "LICENSE.txt"), os.path.join(out_dir, "LICENSE.txt"))
    print("%s: %d modules, %d bytes" % (out, len(modules), os.path.getsize(out)))


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("usage: bundle-novnc.py <extracted package dir> <out dir>")
    main(sys.argv[1], sys.argv[2])
