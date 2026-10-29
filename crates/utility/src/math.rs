pub fn token_amount_to_ui_amount(amount: u64, decimals: u8) -> f64 {
    let divisor = 10u64.pow(decimals as u32) as f64;
    amount as f64 / divisor
}

pub fn ui_amount_to_token_amount(ui_amount: f64, decimals: u8) -> u64 {
    let multiplier = 10u64.pow(decimals as u32) as f64;
    (ui_amount * multiplier).round() as u64
}

/// Oracle price as a float: `raw_price * 10^exponent`.
pub fn to_ui_price(raw_price: i64, exponent: i32) -> f64 {
    raw_price as f64 * 10f64.powi(exponent)
}

pub fn to_ui_price_compact(raw_price: i64, exponent: i32) -> String {
    compact_amount(to_ui_price(raw_price, exponent))
}

const COMPACT_SUFFIXES: [&str; 5] = ["", "k", "M", "B", "T"];

/// Render a UI value compactly: `53.2M`, `1.35B`, `947.5`. Decimal
/// thresholds (1k = 1_000), which is how balances are read, not binary.
pub fn compact_amount(value: f64) -> String {
    if !value.is_finite() {
        return "n/a".to_string();
    }

    let mut scaled = value;
    let mut unit = 0;

    // 999.995 rather than 1000.0: at exactly 1000 the two-decimal format of
    // 999_999 would be "1000k" instead of promoting to "1M".
    while scaled.abs() >= 999.995 && unit < COMPACT_SUFFIXES.len() - 1 {
        scaled /= 1000.0;
        unit += 1;
    }

    // Sub-unit values need more precision or they collapse to "0".
    let decimals = if unit == 0 && scaled != 0.0 && scaled.abs() < 1.0 {
        6
    } else {
        2
    };

    format!("{}{}", trim_zeros(scaled, decimals), COMPACT_SUFFIXES[unit])
}

/// Fixed-precision format with trailing zeros and any bare trailing dot
/// removed, so 53.20 renders as 53.2 and 12.00 as 12.
fn trim_zeros(value: f64, decimals: usize) -> String {
    let s = format!("{value:.decimals$}");
    if !s.contains('.') {
        return s;
    }
    s.trim_end_matches('0').trim_end_matches('.').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compact_amount() {
        assert_eq!(compact_amount(0.0), "0");
        assert_eq!(compact_amount(0.000123), "0.000123");
        assert_eq!(compact_amount(0.000123456789), "0.000123");
        assert_eq!(compact_amount(999.99), "999.99");
        assert_eq!(compact_amount(999.995), "1k");
        assert_eq!(compact_amount(1_234.56), "1.23k");
        assert_eq!(compact_amount(1_234_567.89), "1.23M");
        assert_eq!(compact_amount(1_234_567_890.12), "1.23B");
        assert_eq!(compact_amount(1_234_567_890_123.45), "1.23T");
        assert_eq!(compact_amount(12.00), "12");
    }
}
