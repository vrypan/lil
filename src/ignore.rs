//! `.nolil` ignore-pattern parsing and matching. Supports gitignore-style
//! glob patterns (anchoring, negation, directory-only, `**`). Also defines
//! the built-in exclusions for OS metadata files and the `.lil` state dir.

use std::fs;
use std::io;
use std::path::Path;

pub(crate) const STATE_DIR: &str = ".lil";
pub(crate) const IGNORE_FILE_NAME: &str = ".nolil";
const IGNORED_FILE_NAMES: &[&str] = &[".DS_Store", "Thumbs.db", "Desktop.ini"];
const IGNORED_FILE_PREFIXES: &[&str] = &["._"];
const IGNORED_DIR_NAMES: &[&str] = &[
    ".Spotlight-V100",
    ".Trashes",
    ".fseventsd",
    "$RECYCLE.BIN",
    "lost+found",
];

#[derive(Clone, Debug)]
pub(crate) struct IgnorePattern {
    pattern: String,
    negated: bool,
    directory_only: bool,
    anchored: bool,
    has_slash: bool,
}

pub(crate) fn load_ignore_patterns(root: &Path) -> io::Result<Vec<IgnorePattern>> {
    match fs::read_to_string(root.join(IGNORE_FILE_NAME)) {
        Ok(contents) => Ok(parse_ignore_patterns(&contents)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(err) => Err(err),
    }
}

pub(crate) fn parse_ignore_patterns(contents: &str) -> Vec<IgnorePattern> {
    let mut patterns = Vec::new();
    for raw_line in contents.lines() {
        let mut line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let negated = line.starts_with('!');
        if negated {
            line = &line[1..];
        }

        let directory_only = line.ends_with('/');
        if directory_only {
            line = line.trim_end_matches('/');
        }

        let anchored = line.starts_with('/');
        if anchored {
            line = &line[1..];
        }

        if line.is_empty() {
            continue;
        }

        patterns.push(IgnorePattern {
            pattern: line.to_string(),
            negated,
            directory_only,
            anchored,
            has_slash: line.contains('/'),
        });
    }
    patterns
}

pub(crate) fn matches_ignore_patterns(patterns: &[IgnorePattern], relative: &str) -> bool {
    let mut ignored = false;
    for pattern in patterns {
        if matches_ignore_pattern(pattern, relative) {
            ignored = !pattern.negated;
        }
    }
    ignored
}

fn matches_ignore_pattern(pattern: &IgnorePattern, relative: &str) -> bool {
    let targets = if pattern.directory_only {
        directory_candidates(relative)
    } else {
        vec![relative.to_string()]
    };

    for target in targets {
        if pattern.anchored {
            if glob_match(&pattern.pattern, &target) {
                return true;
            }
            continue;
        }

        if pattern.has_slash {
            for suffix in path_suffix_candidates(&target) {
                if glob_match(&pattern.pattern, suffix) {
                    return true;
                }
            }
        } else {
            for component in target.split('/') {
                if glob_match(&pattern.pattern, component) {
                    return true;
                }
            }
        }
    }
    false
}

fn directory_candidates(relative: &str) -> Vec<String> {
    let parts: Vec<&str> = relative.split('/').collect();
    let mut out = Vec::with_capacity(parts.len());
    for i in 0..parts.len() {
        out.push(parts[..=i].join("/"));
    }
    out
}

fn path_suffix_candidates(relative: &str) -> Vec<&str> {
    let mut suffixes = Vec::new();
    let mut start = 0usize;
    suffixes.push(relative);
    while let Some(pos) = relative[start..].find('/') {
        start += pos + 1;
        suffixes.push(&relative[start..]);
    }
    suffixes
}

fn glob_match(pattern: &str, text: &str) -> bool {
    glob_match_bytes(pattern.as_bytes(), text.as_bytes())
}

fn glob_match_bytes(pattern: &[u8], text: &[u8]) -> bool {
    if pattern.is_empty() {
        return text.is_empty();
    }
    if pattern.starts_with(b"**") {
        let rest = &pattern[2..];
        if glob_match_bytes(rest, text) {
            return true;
        }
        return !text.is_empty() && glob_match_bytes(pattern, &text[1..]);
    }
    match pattern[0] {
        b'*' => {
            let rest = &pattern[1..];
            if glob_match_bytes(rest, text) {
                return true;
            }
            !text.is_empty() && text[0] != b'/' && glob_match_bytes(pattern, &text[1..])
        }
        b'?' => !text.is_empty() && text[0] != b'/' && glob_match_bytes(&pattern[1..], &text[1..]),
        ch => !text.is_empty() && ch == text[0] && glob_match_bytes(&pattern[1..], &text[1..]),
    }
}

pub(crate) fn should_ignore(relative: &str, ignore_patterns: &[IgnorePattern]) -> bool {
    if relative == IGNORE_FILE_NAME {
        return false;
    }

    Path::new(relative).components().any(|component| {
        let name = component.as_os_str().to_string_lossy();
        should_ignore_component(&name)
    }) || matches_ignore_patterns(ignore_patterns, relative)
}

pub(crate) fn should_ignore_component(name: &str) -> bool {
    name == STATE_DIR
        || IGNORED_FILE_NAMES.contains(&name)
        || IGNORED_FILE_PREFIXES
            .iter()
            .any(|prefix| name.starts_with(prefix))
        || IGNORED_DIR_NAMES.contains(&name)
}
