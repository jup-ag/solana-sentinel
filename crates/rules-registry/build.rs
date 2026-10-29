fn main() {
    // `include_shipstern_parser!` emits `#[cfg_attr(feature = "serde", ...)]` on generated
    // types; register the cfg so `unexpected_cfgs` stays quiet without a workspace-wide allow.
    println!("cargo::rustc-check-cfg=cfg(feature, values(\"serde\"))");
}
