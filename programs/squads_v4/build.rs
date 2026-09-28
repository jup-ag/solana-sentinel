fn main() {
    // `include_shipstern_parser!` emits `#[cfg_attr(feature = "serde", ...)]`
    // on generated types; register the cfg here so `unexpected_cfgs` does not
    // warn, without suppressing that lint for the whole workspace.
    println!("cargo::rustc-check-cfg=cfg(feature, values(\"serde\"))");
}
