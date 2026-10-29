use std::process::Command;

fn valsd(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_valsd")).args(args).output().unwrap()
}

#[test]
fn version_prints_build_identification_and_succeeds() {
    for args in [&["--version"][..], &["version"][..]] {
        let output = valsd(args);
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(stdout.contains(concat!("valsd ", env!("CARGO_PKG_VERSION"))), "{stdout}");
        assert!(stdout.contains(" features: "), "{stdout}");
    }
}

#[test]
fn help_succeeds_and_unknown_flags_fail_with_usage() {
    for args in [&["--help"][..], &["-h"][..], &["help"][..]] {
        let output = valsd(args);
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        assert!(String::from_utf8_lossy(&output.stdout).contains("usage:"));
    }
    let output = valsd(&["--bogus"]);
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("usage:"));
    assert!(output.stdout.is_empty());
    assert_eq!(valsd(&[]).status.code(), Some(2));
}

#[test]
fn subcommand_help_succeeds() {
    let output = valsd(&["node", "--help"]);
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("valsd node"));
    let output = valsd(&["admin", "--help"]);
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("pd-replace"));
}
