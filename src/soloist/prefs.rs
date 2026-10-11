//! Soloist's audio preferences: lossless, and no loudness normalisation.
//!
//! Soloist plays Ogg Vorbis 160 with normalisation unless told otherwise in
//! each account's `prefs` file under `<data>/settings/Users/<id>-user/`.
//! Spotify's FLAC is what makes this engine worth running, and normalisation
//! would change the samples, so both are set before Soloist starts.

use std::fs;
use std::path::Path;

use anyhow::Result;

/// The values Spotifast needs. `5` is Spotify's enumeration for lossless.
const PREFS: [(&str, &str); 3] = [
    ("audio.play_bitrate_enumeration", "5"),
    ("audio.play_bitrate_non_metered_enumeration", "5"),
    ("audio.normalize_v2", "false"),
];

/// Applies [`PREFS`] to every account Soloist knows. Returns whether a file
/// changed, which matters only to a Soloist already running: it reads them
/// when it logs in.
pub(crate) fn apply(data: &Path) -> Result<bool> {
    let users = data.join("settings").join("Users");
    let Ok(entries) = fs::read_dir(&users) else {
        return Ok(false);
    };
    let mut changed = false;
    for entry in entries.flatten() {
        let dir = entry.path();
        let is_user = dir
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with("-user"));
        if !is_user || !dir.is_dir() {
            continue;
        }
        let path = dir.join("prefs");
        let current = fs::read_to_string(&path).unwrap_or_default();
        let wanted = with_prefs(&current);
        if wanted != current {
            super::install::write_atomically(&path, wanted.as_bytes())?;
            changed = true;
        }
    }
    Ok(changed)
}

/// Whether Soloist kept an account in `data` from an earlier run.
pub(crate) fn has_account(data: &Path) -> bool {
    fs::read_dir(data.join("settings").join("Users")).is_ok_and(|entries| {
        entries.flatten().any(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.ends_with("-user"))
                && entry.path().is_dir()
        })
    })
}

/// `current` with each of [`PREFS`] set, everything else kept in order.
fn with_prefs(current: &str) -> String {
    let mut out = String::with_capacity(current.len() + 128);
    let mut seen = [false; PREFS.len()];
    for line in current.lines() {
        let key = line.split_once('=').map_or(line, |(key, _)| key).trim();
        match PREFS.iter().position(|(wanted, _)| *wanted == key) {
            Some(index) if !seen[index] => {
                seen[index] = true;
                let (key, value) = PREFS[index];
                out.push_str(&format!("{key}={value}\n"));
            }
            // A second copy of a key would leave its meaning to Soloist.
            Some(_) => {}
            None => {
                out.push_str(line);
                out.push('\n');
            }
        }
    }
    for (index, (key, value)) in PREFS.iter().enumerate() {
        if !seen[index] {
            out.push_str(&format!("{key}={value}\n"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sets_lossless_and_keeps_the_rest() {
        let current = "audio.play_bitrate_non_metered_migrated=true\n\
                       audio.play_bitrate_enumeration=2\n\
                       audio.normalize_v2=true\n\
                       audio.normalize_v2=true\n";
        assert_eq!(
            with_prefs(current),
            "audio.play_bitrate_non_metered_migrated=true\n\
             audio.play_bitrate_enumeration=5\n\
             audio.normalize_v2=false\n\
             audio.play_bitrate_non_metered_enumeration=5\n"
        );
        let done = with_prefs(current);
        assert_eq!(with_prefs(&done), done);
        assert_eq!(with_prefs("").lines().count(), 3);
    }

    #[test]
    fn writes_each_account_once() {
        let data = std::env::temp_dir().join(format!("spotifast-prefs-{}", std::process::id()));
        let _ = fs::remove_dir_all(&data);
        let user = data.join("settings/Users/abc-user");
        fs::create_dir_all(data.join("settings/Users/not-an-account")).unwrap();
        assert!(!has_account(&data));
        fs::create_dir_all(&user).unwrap();
        assert!(has_account(&data));
        fs::write(user.join("prefs"), "audio.play_bitrate_enumeration=2\n").unwrap();
        assert!(apply(&data).unwrap());
        assert!(!apply(&data).unwrap());
        let prefs = fs::read_to_string(user.join("prefs")).unwrap();
        assert!(prefs.contains("audio.play_bitrate_enumeration=5\n"));
        assert!(!data.join("settings/Users/not-an-account/prefs").exists());
        assert!(!apply(&data.join("missing")).unwrap());
        assert!(!has_account(&data.join("missing")));
        let _ = fs::remove_dir_all(data);
    }
}
