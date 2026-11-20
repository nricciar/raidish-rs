//! What `parse_root` accepts.

use super::*;

#[test]
fn reads_one_root() {
    let el = parse_root(
        "<?xml version='1.0'?>\n<!-- hi -->\n<a x='1'><b>t&amp;u</b><c><![CDATA[v]]></c></a>\n",
    )
    .unwrap();
    assert_eq!(el.name, "a");
    assert_eq!(el.attr("x"), Some("1"));
    assert_eq!(el.text_of("b").as_deref(), Some("t&u"));
    assert_eq!(el.text_of("c").as_deref(), Some("v"));
}

#[test]
fn refuses_more_than_one_root() {
    for xml in ["<a/><b/>", "<a></a><b/>", "<a/><b></b>", "<a></a><a></a>"] {
        assert!(parse_root(xml).is_err(), "{}", xml);
    }
}

#[test]
fn refuses_text_outside_the_root() {
    for xml in ["junk<a/>", "<a/>junk", "<a/><![CDATA[x]]>"] {
        assert!(parse_root(xml).is_err(), "{}", xml);
    }
}

#[test]
fn refuses_unknown_entities() {
    assert!(parse_root("<a>&bogus;</a>").is_err());
    assert!(parse_root("<a><b>x &lt; y</b></a>").is_ok());
}

#[test]
fn refuses_deep_nesting() {
    let ok = format!("{}{}", "<a>".repeat(MAX_DEPTH), "</a>".repeat(MAX_DEPTH));
    assert!(parse_root(&ok).is_ok());
    let deep = format!(
        "{}{}",
        "<a>".repeat(MAX_DEPTH + 1),
        "</a>".repeat(MAX_DEPTH + 1)
    );
    assert!(parse_root(&deep).is_err());
    // Far deeper than any stack would take, refused all the same.
    assert!(parse_root(&"<a>".repeat(1_000_000)).is_err());
}
