//! XML as a tree (quick_xml underneath): what libvirt's domain XML and
//! S3's request bodies are read into, and (for libvirt) written back from
//! with whatever raidish doesn't touch kept as it was.

use quick_xml::events::{BytesEnd, BytesStart, Event};

/// Why some XML couldn't be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XmlError(pub String);

impl XmlError {
    pub fn new(e: impl ToString) -> Self {
        XmlError(e.to_string())
    }
}

/// For callers that only report it.
impl From<XmlError> for String {
    fn from(e: XmlError) -> String {
        e.0
    }
}

impl std::fmt::Display for XmlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug)]
pub(crate) enum Node {
    El(El),
    /// Text, comments, CDATA... written back as they were read.
    Raw(Event<'static>),
}

#[derive(Clone, Debug, Default)]
pub(crate) struct El {
    pub(crate) name: String,
    pub(crate) attrs: Vec<(String, String)>,
    pub(crate) kids: Vec<Node>,
}

impl El {
    pub(crate) fn new(name: &str, attrs: &[(&str, &str)]) -> El {
        El {
            name: name.into(),
            attrs: attrs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            kids: Vec::new(),
        }
    }
    pub(crate) fn attr(&self, k: &str) -> Option<&str> {
        self.attrs
            .iter()
            .find(|(n, _)| n == k)
            .map(|(_, v)| v.as_str())
    }
    pub(crate) fn local(&self) -> &str {
        self.name.rsplit(':').next().unwrap_or(&self.name)
    }
    pub(crate) fn child(&self, name: &str) -> Option<&El> {
        self.elements().find(|e| e.name == name)
    }
    pub(crate) fn child_mut(&mut self, name: &str) -> Option<&mut El> {
        self.kids.iter_mut().find_map(|k| match k {
            Node::El(e) if e.name == name => Some(e),
            _ => None,
        })
    }
    /// Its text (what's between its tags, not its children's).
    pub(crate) fn text(&self) -> String {
        let mut t = String::new();
        for k in &self.kids {
            match k {
                Node::Raw(Event::Text(x)) => {
                    t.push_str(&x.unescape().map(|c| c.into_owned()).unwrap_or_default())
                }
                Node::Raw(Event::CData(x)) => t.push_str(&String::from_utf8_lossy(x)),
                _ => {}
            }
        }
        t.trim().to_string()
    }
    pub(crate) fn elements(&self) -> impl Iterator<Item = &El> {
        self.kids.iter().filter_map(|k| match k {
            Node::El(e) => Some(e),
            _ => None,
        })
    }
    pub(crate) fn remove(&mut self, name: &str) {
        self.kids
            .retain(|k| !matches!(k, Node::El(e) if e.name == name));
    }

    /// The first child whose local name (without a namespace prefix) is
    /// `name`.
    pub(crate) fn find(&self, name: &str) -> Option<&El> {
        self.elements().find(|e| e.local() == name)
    }
    /// Every child whose local name is `name`.
    pub(crate) fn find_all<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a El> + 'a {
        self.elements().filter(move |e| e.local() == name)
    }
    /// The text of the child `find` finds.
    pub(crate) fn text_of(&self, name: &str) -> Option<String> {
        self.find(name).map(El::text)
    }
}

fn start_el(e: &BytesStart) -> Result<El, XmlError> {
    let name = String::from_utf8_lossy(e.name().as_ref()).into_owned();
    let mut attrs = Vec::new();
    for a in e.attributes() {
        let a = a.map_err(XmlError::new)?;
        attrs.push((
            String::from_utf8_lossy(a.key.as_ref()).into_owned(),
            a.unescape_value().map_err(XmlError::new)?.into_owned(),
        ));
    }
    Ok(El {
        name,
        attrs,
        kids: Vec::new(),
    })
}

/// How deeply elements may nest (well past any domain XML or S3 body;
/// the tree is walked recursively).
pub(crate) const MAX_DEPTH: usize = 128;

/// The root element of `xml`: exactly one, nested at most [`MAX_DEPTH`]
/// deep, with nothing but whitespace, comments and declarations around it,
/// and every entity in its text resolvable.
pub(crate) fn parse_root(xml: &str) -> Result<El, XmlError> {
    let mut reader = quick_xml::Reader::from_str(xml);
    let mut stack: Vec<El> = Vec::new();
    let mut root: Option<El> = None;
    let at = |r: &quick_xml::Reader<&[u8]>| r.buffer_position();
    loop {
        let ev = reader
            .read_event()
            .map_err(|e| XmlError::new(format!("XML at byte {}: {}", at(&reader), e)))?;
        // A finished element: its parent's child, or the root.
        let done = match ev {
            Event::Start(s) => {
                if stack.is_empty() && root.is_some() {
                    return Err(XmlError::new("XML: more than one root element"));
                }
                if stack.len() >= MAX_DEPTH {
                    return Err(XmlError::new(format!(
                        "XML: nested more than {} deep",
                        MAX_DEPTH
                    )));
                }
                stack.push(start_el(&s)?);
                None
            }
            Event::Empty(s) => Some(start_el(&s)?),
            Event::End(_) => Some(
                stack
                    .pop()
                    .ok_or_else(|| XmlError::new("XML: unbalanced end tag"))?,
            ),
            Event::Eof => break,
            Event::Text(t) if stack.is_empty() => {
                if !t.iter().all(u8::is_ascii_whitespace) {
                    return Err(XmlError::new("XML: text outside the root element"));
                }
                None
            }
            Event::CData(_) if stack.is_empty() => {
                return Err(XmlError::new("XML: text outside the root element"));
            }
            Event::Decl(_) | Event::DocType(_) | Event::PI(_) | Event::Comment(_)
                if stack.is_empty() =>
            {
                None
            }
            ev => {
                if let Event::Text(t) = &ev {
                    t.unescape().map_err(|e| {
                        XmlError::new(format!("XML at byte {}: {}", at(&reader), e))
                    })?;
                }
                if let Some(p) = stack.last_mut() {
                    p.kids.push(Node::Raw(ev.into_owned()));
                }
                None
            }
        };
        if let Some(el) = done {
            match stack.last_mut() {
                Some(p) => p.kids.push(Node::El(el)),
                None if root.is_some() => {
                    return Err(XmlError::new("XML: more than one root element"));
                }
                None => root = Some(el),
            }
        }
    }
    if !stack.is_empty() {
        return Err(XmlError::new("XML: unclosed element"));
    }
    root.ok_or_else(|| XmlError::new("XML: no root element"))
}

fn write_el(w: &mut quick_xml::Writer<Vec<u8>>, el: &El) -> std::io::Result<()> {
    let mut start = BytesStart::new(el.name.as_str());
    for (k, v) in &el.attrs {
        start.push_attribute((k.as_str(), v.as_str()));
    }
    if el.kids.is_empty() {
        return w.write_event(Event::Empty(start));
    }
    w.write_event(Event::Start(start))?;
    for k in &el.kids {
        match k {
            Node::El(e) => write_el(w, e)?,
            Node::Raw(ev) => w.write_event(ev.clone())?,
        }
    }
    w.write_event(Event::End(BytesEnd::new(el.name.as_str())))
}

/// `el` as XML.
pub(crate) fn el_xml(el: &El) -> String {
    let mut w = quick_xml::Writer::new(Vec::new());
    write_el(&mut w, el).expect("writing to memory");
    String::from_utf8(w.into_inner()).expect("utf-8 in, utf-8 out")
}

#[cfg(test)]
#[path = "tests/xml_tests.rs"]
mod tests;
