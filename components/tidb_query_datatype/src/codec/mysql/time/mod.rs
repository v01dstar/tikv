// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

pub mod extension;
pub mod interval;
mod tz;
pub mod weekmode;

use std::{
    cmp::Ordering,
    convert::{TryFrom, TryInto},
    fmt::Write,
    hash::{Hash, Hasher},
    intrinsics::unlikely,
};

use bitfield::bitfield;
use boolinator::Boolinator;
use chrono::prelude::*;
use codec::prelude::*;
use tipb::FieldType;

pub use self::{extension::*, interval::IntervalUnit, tz::Tz, weekmode::WeekMode};
use crate::{
    FieldTypeAccessor, FieldTypeTp,
    codec::{
        Error, Result, TEN_POW,
        convert::ConvertTo,
        data_type::Real,
        mysql::{DEFAULT_FSP, Decimal, Duration, MAX_FSP, Res, check_fsp, duration::*},
    },
    expr::{EvalContext, Flag, SqlMode},
};

const MIN_TIMESTAMP: i64 = 0;
pub const MAX_TIMESTAMP: i64 = (1 << 31) - 1;
const MICRO_WIDTH: usize = 6;
const MAX_COMPONENTS_LEN: usize = 9;
pub const MIN_YEAR: u32 = 1901;
pub const MAX_YEAR: u32 = 2155;

pub const MONTH_NAMES: &[&str] = &[
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];

const MONTH_NAMES_ABBR: &[&str] = &[
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

fn is_leap_year(year: u32) -> bool {
    year & 3 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn last_day_of_month(year: u32, month: u32) -> u32 {
    match month {
        4 | 6 | 9 | 11 => 30,
        2 => is_leap_year(year) as u32 + 28,
        _ => 31,
    }
}

/// Round each component.
/// ```ignore
/// let mut parts = [2019, 12, 1, 23, 59, 59, 1000000];
/// round_components(&mut parts);
/// assert_eq!([2019, 12, 2, 0, 0, 0, 0], parts);
/// ```
/// When year, month or day is zero, there can not have a carry.
/// e.g.: `"1998-11-00 23:59:59.999" (fsp = 2, round = true)`, in `hms` it
/// contains a carry, however, the `day` is 0, which is invalid in `MySQL`. When
/// thoese cases encountered, return None.
fn round_components(parts: &mut [u32]) -> Option<()> {
    debug_assert_eq!(parts.len(), 7);
    let modulus = [
        u32::MAX,
        12,
        last_day_of_month(parts[0], parts[1]),
        // hms[.fraction]
        24,
        60,
        60,
        1_000_000,
    ];
    for i in (1..=6).rev() {
        let is_ymd = u32::from(i < 3);
        if parts[i] >= modulus[i] + is_ymd {
            parts[i] -= modulus[i];
            if i < 4 && parts[i - 1] == 0 || parts[i - 1] > modulus[i - 1] {
                return None;
            }
            parts[i - 1] += 1;
        }
    }
    Some(())
}

#[inline]
#[allow(clippy::too_many_arguments)]
fn chrono_datetime<T: TimeZone>(
    time_zone: &T,
    year: u32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    micro: u32,
) -> Result<DateTime<T>> {
    // NOTE: We are not using `tz::from_ymd_opt` as suggested in chrono's README due
    // to chronotope/chrono-tz #23.
    // As a workaround, we first build a NaiveDate, then attach time zone
    // information to it.
    NaiveDate::from_ymd_opt(year as i32, month, day)
        .and_then(|date| date.and_hms_opt(hour, minute, second))
        .and_then(|t| t.checked_add_signed(chrono::Duration::microseconds(i64::from(micro))))
        .and_then(|datetime| time_zone.from_local_datetime(&datetime).earliest())
        .ok_or_else(Error::truncated)
}

#[inline]
fn chrono_naive_datetime(
    year: u32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    micro: u32,
) -> Result<NaiveDateTime> {
    NaiveDate::from_ymd_opt(year as i32, month, day)
        .and_then(|date| date.and_hms_opt(hour, minute, second))
        .and_then(|t| t.checked_add_signed(chrono::Duration::microseconds(i64::from(micro))))
        .ok_or_else(Error::truncated)
}

/// Round `frac` with `fsp`, return if there is a carry and the result.
/// NOTE: we assume that `frac` is less than `100_000_000` and `fsp` is valid.
/// ```ignore
/// assert_eq!(123460, round_frac(123456, 5));
/// assert_eq!(1_000_000, round_frac(999999, 5));
/// assert_eq!(1230, round_frac(1234, 5)); // .001234, fsp = 5 => .001230
/// ```
fn round_frac(frac: u32, fsp: u8) -> (bool, u32) {
    debug_assert!(frac < 100_000_000);
    debug_assert!(fsp < 7);
    if frac < 1_000_000 && fsp == 6 {
        return (false, frac);
    }

    let fsp = usize::from(fsp);
    let width: usize = if frac >= 1_000_000 { 7 } else { 6 };
    let mask = TEN_POW[width - fsp - 1];
    let result = (frac / mask + 5) / 10 * mask * if width == 6 { 10 } else { 1 };
    (result >= 1_000_000, result)
}

bitfield! {
    #[derive(Clone, Copy, Default)]
    pub struct Time(u64);

    u32;
    #[inline]
    get_year, set_year: 63, 50;
    #[inline]
    get_month, set_month: 49, 46;
    #[inline]
    get_day, set_day: 45, 41;
    #[inline]
    get_hour, set_hour: 40, 36;
    #[inline]
    get_minute, set_minute: 35, 30;
    #[inline]
    get_second,set_second: 29, 24;
    #[inline]
    get_micro, set_micro: 23, 4;

    // `fsp_tt` format:
    // | fsp: 3 bits | type: 1 bit |
    // When `fsp` is valid (in range [0, 6]):
    // 1. `type` bit 0 represent `DateTime`
    // 2. `type` bit 1 represent `Timestamp`
    //
    // Since `Date` does not require `fsp`, we could use `fsp_tt` == 0b1110 to represent it.
    #[inline]
    u8, get_fsp_tt, set_fsp_tt: 3, 0;
}

#[derive(PartialEq, Clone, Copy, Debug)]
pub enum TimeType {
    Date,
    DateTime,
    Timestamp,
}

impl TryFrom<FieldTypeTp> for TimeType {
    type Error = crate::codec::Error;
    fn try_from(time_type: FieldTypeTp) -> Result<TimeType> {
        Ok(match time_type {
            FieldTypeTp::Date => TimeType::Date,
            FieldTypeTp::DateTime => TimeType::DateTime,
            FieldTypeTp::Timestamp => TimeType::Timestamp,
            // TODO: Remove the support of transfering `Unspecified` to `DateTime`
            FieldTypeTp::Unspecified => TimeType::DateTime,
            _ => return Err(box_err!("Time does not support field type {}", time_type)),
        })
    }
}

impl From<TimeType> for FieldTypeTp {
    fn from(time_type: TimeType) -> FieldTypeTp {
        match time_type {
            TimeType::Timestamp => FieldTypeTp::Timestamp,
            TimeType::DateTime => FieldTypeTp::DateTime,
            TimeType::Date => FieldTypeTp::Date,
        }
    }
}

// The common set of methods for `date/time`
impl Time {
    /// Returns the hour number from 0 to 23.
    #[inline]
    pub fn hour(self) -> u32 {
        self.get_hour()
    }

    /// Returns the minute number from 0 to 59.
    #[inline]
    pub fn minute(self) -> u32 {
        self.get_minute()
    }

    /// Returns the second number from 0 to 59.
    #[inline]
    pub fn second(self) -> u32 {
        self.get_second()
    }

    /// Returns the number of microseconds since the whole second.
    pub fn micro(self) -> u32 {
        self.get_micro()
    }

    /// Returns the year number
    pub fn year(self) -> u32 {
        self.get_year()
    }

    /// Returns the month number
    pub fn month(self) -> u32 {
        self.get_month()
    }

    /// Returns the day number
    pub fn day(self) -> u32 {
        self.get_day()
    }

    /// used to convert period to month
    pub fn period_to_month(period: u64) -> u64 {
        if period == 0 {
            return 0;
        }
        let (year, month) = (period / 100, period % 100);
        if year < 70 {
            (year + 2000) * 12 + month - 1
        } else if year < 100 {
            (year + 1900) * 12 + month - 1
        } else {
            year * 12 + month - 1
        }
    }

    /// used to convert month to period
    pub fn month_to_period(month: u64) -> u64 {
        if month == 0 {
            return 0;
        }
        let year = month / 12;
        if year < 70 {
            (year + 2000) * 100 + month % 12 + 1
        } else if year < 100 {
            (year + 1900) * 100 + month % 12 + 1
        } else {
            year * 100 + month % 12 + 1
        }
    }
}

mod parser {
    use super::*;

    fn bytes_to_u32(input: &[u8]) -> Option<u32> {
        input.iter().try_fold(0u32, |acc, d| {
            d.is_ascii_digit().as_option()?;
            acc.checked_mul(10)
                .and_then(|t| t.checked_add(u32::from(d - b'0')))
        })
    }

    /// Match at least one digit and return the rest of the slice.
    /// ```ignore
    ///  digit1(b"12:32") == Some((b":32", b"12"))
    ///  digit1(b":32") == None
    /// ```
    fn digit1(input: &[u8]) -> Option<(&[u8], &[u8])> {
        let end = input
            .iter()
            .position(|&c| !c.is_ascii_digit())
            .unwrap_or(input.len());
        (end != 0).as_option()?;
        Some((&input[end..], &input[..end]))
    }

    /// Match at least one space and return the rest of the slice.
    /// ```ignore
    ///  space1(b"    12:32") == Some((b"    ", b"12:32"))
    ///  space1(b":32") == None
    /// ```
    fn space1(input: &[u8]) -> Option<(&[u8], &[u8])> {
        let end = input.iter().position(|&c| !c.is_ascii_whitespace())?;

        Some((&input[end..], &input[..end]))
    }

    /// Match at least one ascii punctuation and return the rest of the slice.
    /// ```ignore
    ///  punct1(b"..10") == Some((b"..", b"10"))
    ///  punct1(b"10:32") == None
    /// ```
    fn punct1(input: &[u8]) -> Option<(&[u8], &[u8])> {
        let end = input.iter().position(|&c| !c.is_ascii_punctuation())?;

        Some((&input[end..], &input[..end]))
    }

    /// We assume that the `input` is trimmed and is not empty.
    /// ```ignore
    ///  split_components_with_tz(b"2020-12-24T15:37:50+0800")?.1 == Some(480*60)
    /// ```
    /// the second value if not None indicates the offset in seconds of the
    /// timezone parsed
    fn split_components_with_tz(input: &str) -> Option<(Vec<&[u8]>, Option<i32>)> {
        let mut buffer = input.as_bytes();

        debug_assert!(
            !buffer.is_empty()
                && !buffer.first().unwrap().is_ascii_whitespace()
                && !buffer.last().unwrap().is_ascii_whitespace()
        );

        let mut components = Vec::with_capacity(MAX_COMPONENTS_LEN);
        let mut separators = Vec::with_capacity(MAX_COMPONENTS_LEN - 1);

        while !buffer.is_empty() {
            let (mut rest, digits): (&[u8], &[u8]) = digit1(buffer)?;

            components.push(digits);

            if !rest.is_empty() {
                // If a whitespace is acquired, we expect we have already collected ymd.
                if rest[0].is_ascii_whitespace() {
                    (components.len() == 3).as_option()?;
                    let result = space1(rest)?;
                    rest = result.0;
                    separators.push(result.1);
                }
                // If a 'T' is acquired, we expect we have already collected ymd.
                else if rest[0] == b'T' {
                    (components.len() == 3).as_option()?;
                    separators.push(&rest[..1]);
                    rest = &rest[1..];
                }
                // If a 'Z' is acquired, we expect that we are parsing timezone now.
                // the time should be in ISO8601 format, e.g. 2020-10-10T19:27:10Z, so there should
                // be 6 part ahead or 7 if considering fsp.
                else if rest[0] == b'Z' {
                    (components.len() == 6 || components.len() == 7).as_option()?;
                    separators.push(&rest[..1]);
                    rest = &rest[1..];
                }
                // If a punctuation is acquired, move forward the pointer. Note that we should
                // consume multiple punctuations if existing because MySQL allows to parse time
                // like 2020--12..16T18::58^^45.
                else if rest[0].is_ascii_punctuation() {
                    let result = punct1(rest)?;
                    separators.push(result.1);
                    rest = result.0;
                } else {
                    return None;
                }
            }

            buffer = rest;
        }

        let mut tz_offset = 0i32;
        let mut tz_sign: &[u8] = b"";
        let mut tz_hour: &[u8] = b"";
        let mut tz_minute: &[u8] = b"";
        let mut has_tz = false;
        // the following statement handles timezone
        match components.len() {
            9 => {
                // 2020-12-23 15:59:23.233333+08:00
                (separators.len() == 8).as_option()?;
                match separators[6..] {
                    [b"+", b":"] | [b"-", b":"] => {
                        has_tz = true;
                        tz_sign = separators[6];
                        tz_minute = components.pop()?;
                        tz_hour = components.pop()?;
                    }
                    _ => return None,
                };
            }
            8 => {
                // 2020-12-23 15:59:23.2333-08
                // 2020-12-23 15:59:23.2333-0800
                // 2020-12-23 15:59:23+08:00
                (separators.len() == 7).as_option()?;
                match separators[5..] {
                    [b".", b"-"] | [b".", b"+"] => {
                        has_tz = true;
                        tz_sign = separators[6];
                        tz_hour = components.pop()?;
                    }
                    [b"+", b":"] | [b"-", b":"] => {
                        has_tz = true;
                        tz_sign = separators[5];
                        tz_minute = components.pop()?;
                        tz_hour = components.pop()?;
                    }
                    _ => return None,
                }
            }
            7 => {
                // 2020-12-23 15:59:23.23333Z
                // 2020-12-23 15:59:23+0800
                // 2020-12-23 15:59:23-08
                match separators.len() {
                    7 => {
                        (separators.last()? == b"Z").as_option()?;
                        has_tz = true;
                    }
                    6 => {
                        tz_sign = separators[5];
                        if tz_sign == b"+" || tz_sign == b"-" {
                            has_tz = true;
                            tz_hour = components.pop()?;
                        }
                    }
                    _ => return None, // this branch can never be reached
                }
            }
            6 => {
                // 2020-12-23 15:59:23Z
                if separators.len() == 6 && separators.last()? == b"Z" {
                    has_tz = true;
                }
            }
            _ => {}
        }
        if has_tz {
            if tz_hour.len() == 4 {
                let tmp = tz_hour.split_at(2);
                tz_hour = tmp.0;
                tz_minute = tmp.1;
            }
            ((tz_hour.len() == 2 || tz_hour.is_empty())
                && (tz_minute.len() == 2 || tz_minute.is_empty()))
            .as_option()?;
            let delta_hour = bytes_to_u32(tz_hour)? as i32;
            let delta_minute = bytes_to_u32(tz_minute)? as i32;
            (!(delta_hour > 14
                || delta_minute > 59
                || (delta_hour == 14 && delta_minute != 0)
                || (tz_sign == b"-" && delta_hour == 0 && delta_minute == 0)))
                .as_option()?;
            tz_offset = (delta_hour * 60 + delta_minute) * 60;
            if tz_sign == b"-" {
                tz_offset = -tz_offset;
            }
        }
        // the following statement checks fsp
        ((components.len() != 7 && components.len() != 2)
            || (separators.len() >= components.len() - 1 /* should always true */ && separators[components.len() - 2] == b"."))
            .as_option()?;

        Some((components, if has_tz { Some(tz_offset) } else { None }))
    }

    /// If a two-digit year encountered, add an offset to it.
    /// 99 -> 1999
    /// 20 -> 2020
    fn adjust_year(year: u32) -> u32 {
        if year <= 69 {
            2000 + year
        } else if (70..=99).contains(&year) {
            1900 + year
        } else {
            year
        }
    }

    /// Try to parse a datetime string `input` without fractional part and
    /// separators. return an array that stores `[year, month, day, hour,
    /// minute, second, 0]`
    fn parse_whole(input: &[u8]) -> Option<[u32; 7]> {
        let mut parts = [0u32; 7];

        // If `input`'s len is 8 or 14, then `input` should be in format like:
        // yyyymmdd/yyyymmddhhmmss which means we have a four-digit year.
        // Otherwise, we have a two-digit year.
        let year_digits = match input.len() {
            14 | 8 => 4,
            9..=12 | 5..=7 => 2,
            _ => return None,
        };

        parts[0] = bytes_to_u32(&input[..year_digits])?;
        // If we encounter a two-digit year, translate it to a four-digit year.
        if year_digits == 2 {
            parts[0] = adjust_year(parts[0]);
        }

        for (i, chunk) in input[year_digits..].chunks(2).enumerate() {
            parts[i + 1] = bytes_to_u32(chunk)?;
        }

        Some(parts)
    }

    /// Try to parse a fractional part from `input` with `fsp`, round the result
    /// if `round` is true.
    /// NOTE: This function assumes that `fsp` is in range: [0, 6].
    fn parse_frac(input: &[u8], fsp: u8, round: bool) -> Option<(bool, u32)> {
        debug_assert!(fsp < 7);
        let fsp = usize::from(fsp);
        let len = input.len();

        let (input, len) = if fsp >= input.len() {
            (input, len)
        } else {
            (&input[..fsp + round as usize], fsp + round as usize)
        };

        let frac = bytes_to_u32(input)? * TEN_POW[MICRO_WIDTH.saturating_sub(len)];

        Some(if round {
            round_frac(frac, fsp as u8)
        } else {
            (false, frac)
        })
    }

    pub fn parse(
        ctx: &mut EvalContext,
        input: &str,
        time_type_opt: Option<TimeType>,
        fsp: u8,
        round: bool,
    ) -> Option<Time> {
        let trimmed = input.trim();
        (!trimmed.is_empty()).as_option()?;

        // to support ISO8601 and MySQL's time zone support, we further parse the
        // following formats 2020-12-17T11:55:55Z
        // 2020-12-17T11:55:55+0800
        // 2020-12-17T11:55:55-08
        // 2020-12-17T11:55:55+02:00
        let (components, tz) = split_components_with_tz(trimmed)?;
        // https://github.com/pingcap/tidb/blob/fcf9e5ea75c6a23f80c9246bd7b457a8577774b3/pkg/types/time.go#L2640
        let time_type = if let Some(tt) = time_type_opt {
            tt
        } else {
            match components.len() {
                1 => {
                    let len = components[0].len();
                    if len == 8 || len == 6 || len == 5 {
                        TimeType::Date
                    } else {
                        TimeType::DateTime
                    }
                }
                3 => TimeType::Date,
                _ => TimeType::DateTime,
            }
        };

        let time_without_tz = match components.len() {
            1 | 2 => {
                let mut whole = parse_whole(components[0])?;

                let (carry, frac) = if let Some(frac) = components.get(1) {
                    // If we have a fractional part,
                    // we expect the `whole` is in format: `yymmddhhmm/yymmddhhmmss/yyyymmddhhmmss`.
                    // Otherwise, the fractional part is meaningless.
                    (components[0].len() == 10
                        || components[0].len() == 12
                        || components[0].len() == 14)
                        .as_option()?;
                    parse_frac(frac, fsp, round)?
                } else {
                    (false, 0)
                };

                whole[6] = frac;
                let mut parts = whole;
                if carry {
                    round_components(&mut parts)?;
                }

                Time::from_slice(ctx, &parts, time_type, fsp)
            }
            3..=7 => {
                let whole = std::cmp::min(components.len(), 6);
                let mut parts: Vec<_> = components[..whole].iter().try_fold(
                    Vec::with_capacity(MAX_COMPONENTS_LEN),
                    |mut acc, part| -> Option<_> {
                        acc.push(bytes_to_u32(part)?);
                        Some(acc)
                    },
                )?;

                let (carry, frac) = if let Some(frac) = components.get(6) {
                    parse_frac(frac, fsp, round)?
                } else {
                    (false, 0)
                };

                parts.resize(6, 0);
                parts.push(frac);
                // Skip a special case "00-00-00".
                if components[0].len() == 2 && !parts.iter().all(|x| *x == 0u32) {
                    parts[0] = adjust_year(parts[0]);
                }

                if carry {
                    round_components(&mut parts)?;
                }
                Time::from_slice(ctx, &parts, time_type, fsp)
            }
            _ => None,
        };
        match (tz, time_without_tz) {
            (Some(tz_offset), Some(t)) => {
                let tz_parsed = Tz::from_offset(tz_offset as i64)?;
                let mut ts = chrono_datetime(
                    &tz_parsed,
                    t.year(),
                    t.month(),
                    t.day(),
                    t.hour(),
                    t.minute(),
                    t.second(),
                    t.micro(),
                )
                .ok()?;
                ts = ts.with_timezone(&ctx.cfg.tz);
                Some(
                    Time::try_from_chrono_datetime(ctx, ts.naive_local(), time_type, fsp as i8)
                        .ok()?,
                )
            }
            _ => time_without_tz,
        }
    }

    pub fn parse_from_float_string(
        ctx: &mut EvalContext,
        input: String,
        time_type: TimeType,
        fsp: u8,
        round: bool,
    ) -> Option<Time> {
        let (components, _) = split_components_with_tz(input.as_str())?;
        match components.len() {
            1 | 2 => {
                let result: i64 = components[0].convert(ctx).ok()?;
                let whole_time = parse_from_i64(ctx, result, time_type, fsp)?;
                let mut whole = [
                    whole_time.get_year(),
                    whole_time.get_month(),
                    whole_time.get_day(),
                    whole_time.get_hour(),
                    whole_time.get_minute(),
                    whole_time.get_second(),
                    0,
                ];

                let (carry, frac) = if let Some(frac) = components.get(1) {
                    // If we have a fractional part,
                    // we expect the `whole` is in format: `yymmddhhmmss/yyyymmddhhmmss`,
                    // which match the `whole` part length from 9 to 14.
                    // Otherwise, the fractional part is meaningless.
                    if components[0].len() >= 9 && components[0].len() <= 14 {
                        parse_frac(frac, fsp, round)?
                    } else {
                        (false, 0)
                    }
                } else {
                    (false, 0)
                };

                whole[6] = frac;
                let mut parts = whole;
                if carry {
                    round_components(&mut parts)?;
                }

                Time::from_slice(ctx, &parts, time_type, fsp)
            }
            _ => None,
        }
    }

    /// Try to parse a `i64` into a `Time` with the given type and fsp
    pub fn parse_from_i64(
        ctx: &mut EvalContext,
        input: i64,
        time_type: TimeType,
        fsp: u8,
    ) -> Option<Time> {
        if input == 0 {
            return Time::zero(ctx, fsp as i8, time_type).ok();
        }
        // NOTE: These numbers can be consider as strings
        // The parser eats two digits each time from the end of string,
        // and fill it into `Time` with reversed order.
        // Port from: https://github.com/pingcap/tidb/blob/b1aad071489619998e4caefd235ed01f179c2db2/types/time.go#L1263
        let aligned = match input {
            101..=691_231 => (input + 20_000_000) * 1_000_000,
            700_101..=991_231 => (input + 19_000_000) * 1_000_000,
            991_232..=99_991_231 => input * 1_000_000,
            101_000_000..=691_231_235_959 => input + 20_000_000_000_000,
            700_101_000_000..=991_231_235_959 => input + 19_000_000_000_000,
            1_000_000_000_000..=i64::MAX => input,
            _ => return None,
        };

        Time::from_aligned_i64(ctx, aligned, time_type, fsp as i8).ok()
    }

    pub fn parse_from_i64_default(ctx: &mut EvalContext, input: i64) -> Option<Time> {
        if input == 0 {
            return Time::zero(ctx, DEFAULT_FSP, TimeType::Date).ok();
        }
        // NOTE: These numbers can be consider as strings
        // The parser eats two digits each time from the end of string,
        // and fill it into `Time` with reversed order.
        // Port from: https://github.com/pingcap/tidb/blob/b1aad071489619998e4caefd235ed01f179c2db2/types/time.go#L1263
        let aligned = match input {
            101..=691_231 => (input + 20_000_000) * 1_000_000,
            700_101..=991_231 => (input + 19_000_000) * 1_000_000,
            991_232..=99_991_231 => input * 1_000_000,
            101_000_000..=691_231_235_959 => input + 20_000_000_000_000,
            700_101_000_000..=991_231_235_959 => input + 19_000_000_000_000,
            1_000_000_000_000..=i64::MAX => input,
            _ => return None,
        };

        let time_type = if input >= 101_000_000 {
            TimeType::DateTime
        } else {
            TimeType::Date
        };

        Time::from_aligned_i64(ctx, aligned, time_type, DEFAULT_FSP).ok()
    }

    pub fn parse_from_real_default(ctx: &mut EvalContext, input: &Real) -> Option<Time> {
        let int_part = input.trunc() as i64;
        let mut t = parse_from_i64_default(ctx, int_part)?;
        if t.get_time_type() == TimeType::DateTime {
            let micro = (input.fract() * 1000000.0).round() as u32;
            t.set_fsp(MAX_FSP as u8);
            t.set_micro(micro);
        }
        Some(t)
    }

    pub fn parse_from_decimal_default(ctx: &mut EvalContext, input: &Decimal) -> Option<Time> {
        let int_part = match input.as_i64() {
            Res::Ok(i) | Res::Truncated(i) => i,
            _ => return None,
        };
        let mut t = parse_from_i64_default(ctx, int_part)?;
        let fsp = std::cmp::min(MAX_FSP as u8, input.frac_cnt());
        t.set_fsp(fsp);
        if fsp == 0 || t.get_time_type() == TimeType::Date {
            return Some(t);
        }

        let frac_part_decimal = match input - &int_part.into() {
            Res::Ok(d) => d,
            _ => return None,
        };
        let micro_part_decimal = match &frac_part_decimal * &1000000i64.into() {
            Res::Ok(d) | Res::Truncated(d) => d,
            _ => return None,
        };
        let micro_part = match micro_part_decimal.as_u64() {
            Res::Ok(i) | Res::Truncated(i) => i as u32,
            _ => return None,
        };
        t.set_micro(micro_part);

        Some(t)
    }
}

/// The MySQL time_format rules used in TiDB differ slightly from chrono (e.g.,
/// %c, %f). Therefore, we need to implement our own parser function instead of
/// directly using the implementation of `chrono::Datetime`.
mod date_format_parser {
    use std::collections::HashMap;

    use super::*;
    type DateFormatParser<'a> =
        fn(&mut Time, &'a str, &mut HashMap<String, i64>) -> (&'a str, bool);

    const MONTH_NAMES: [&str; 12] = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ];

    const CONST_FOR_AM: i64 = 1;
    const CONST_FOR_PM: i64 = 2;

    fn parse_n_digits(input: &str, limit: i32) -> (u32, usize) {
        if limit <= 0 {
            return (0, 0);
        }
        let mut num: u32 = 0;
        let mut step: usize = 0;
        for c in input.chars() {
            if step < limit as usize && c.is_ascii_digit() {
                num = num * 10 + c.to_digit(10).unwrap();
            } else {
                break;
            }
            step += 1;
        }
        (num, step)
    }

    fn has_case_insensitive_prefix(input: &str, prefix: &str) -> bool {
        input.len() >= prefix.len() && input[..prefix.len()].eq_ignore_ascii_case(prefix)
    }

    fn abbreviated_month<'a>(
        time: &mut Time,
        input: &'a str,
        _: &mut HashMap<String, i64>,
    ) -> (&'a str, bool) {
        let month_mapper = |month_name: &str| -> Option<u32> {
            match month_name {
                "jan" => Some(1),
                "feb" => Some(2),
                "mar" => Some(3),
                "apr" => Some(4),
                "may" => Some(5),
                "jun" => Some(6),
                "jul" => Some(7),
                "aug" => Some(8),
                "sep" => Some(9),
                "oct" => Some(10),
                "nov" => Some(11),
                "dec" => Some(12),
                _ => None,
            }
        };
        if input.len() >= 3 {
            let month_name = &input[..3].to_lowercase();
            if let Some(month) = month_mapper(month_name) {
                time.set_month(month);
                return (&input[3..], true);
            }
        }
        (input, false)
    }

    fn month_numeric<'a>(
        time: &mut Time,
        input: &'a str,
        _: &mut HashMap<String, i64>,
    ) -> (&'a str, bool) {
        let (month, step) = parse_n_digits(input, 2);
        if step == 0 || month > 12 {
            return (input, false);
        }
        time.set_month(month);
        (&input[step..], true)
    }

    fn day_of_month_numeric<'a>(
        time: &mut Time,
        input: &'a str,
        _: &mut HashMap<String, i64>,
    ) -> (&'a str, bool) {
        let (day, step) = parse_n_digits(input, 2);
        if step == 0 || day > 31 {
            return (input, false);
        }
        time.set_day(day);
        (&input[step..], true)
    }

    fn micro_second<'a>(
        time: &mut Time,
        input: &'a str,
        _: &mut HashMap<String, i64>,
    ) -> (&'a str, bool) {
        let (mut micro, step) = parse_n_digits(input, 6);
        if step == 0 {
            time.set_micro(0);
            return (input, true);
        }
        for _ in step..6 {
            micro *= 10;
        }
        time.set_micro(micro);
        (&input[step..], true)
    }

    fn hour_12_numeric<'a>(
        time: &mut Time,
        input: &'a str,
        ctx: &mut HashMap<String, i64>,
    ) -> (&'a str, bool) {
        let (hour, step) = parse_n_digits(input, 2);
        if step == 0 || hour > 12 || hour == 0 {
            return (input, false);
        }
        time.set_hour(hour);
        ctx.insert("%h".into(), hour as i64);
        (&input[step..], true)
    }

    fn hour_24_numeric<'a>(
        time: &mut Time,
        input: &'a str,
        ctx: &mut HashMap<String, i64>,
    ) -> (&'a str, bool) {
        let (hour, step) = parse_n_digits(input, 2);
        if step == 0 || hour > 23 {
            return (input, false);
        }
        time.set_hour(hour);
        ctx.insert("%H".into(), hour as i64);
        (&input[step..], true)
    }

    fn minute_numeric<'a>(
        time: &mut Time,
        input: &'a str,
        _: &mut HashMap<String, i64>,
    ) -> (&'a str, bool) {
        let (minute, step) = parse_n_digits(input, 2);
        if step == 0 || minute >= 60 {
            return (input, false);
        }
        time.set_minute(minute);
        (&input[step..], true)
    }

    fn day_of_year_numeric<'a>(
        _time: &mut Time,
        input: &'a str,
        ctx: &mut HashMap<String, i64>,
    ) -> (&'a str, bool) {
        // MySQL declares that "%j" should be "Day of year (001..366)". But actually,
        // it accepts a number that is up to three digits, which range is [1, 999].
        let (day, step) = parse_n_digits(input, 3);
        if step == 0 || day == 0 {
            return (input, false);
        }
        ctx.insert("%j".into(), day as i64);
        (&input[step..], true)
    }

    fn full_name_month<'a>(
        time: &mut Time,
        input: &'a str,
        _: &mut HashMap<String, i64>,
    ) -> (&'a str, bool) {
        for (i, month_name) in MONTH_NAMES.iter().enumerate() {
            if has_case_insensitive_prefix(input, month_name) {
                time.set_month(i as u32 + 1);
                return (&input[month_name.len()..], true);
            }
        }
        (input, false)
    }

    fn is_am_or_pm<'a>(
        _time: &mut Time,
        input: &'a str,
        ctx: &mut HashMap<String, i64>,
    ) -> (&'a str, bool) {
        if input.len() < 2 {
            return (input, false);
        }
        match input[..2].to_lowercase().as_str() {
            "am" => {
                ctx.insert("%p".into(), CONST_FOR_AM);
                (&input[2..], true)
            }
            "pm" => {
                ctx.insert("%p".into(), CONST_FOR_PM);
                (&input[2..], true)
            }
            _ => (input, false),
        }
    }

    fn seconds_numeric<'a>(
        time: &mut Time,
        input: &'a str,
        _: &mut HashMap<String, i64>,
    ) -> (&'a str, bool) {
        let (second, step) = parse_n_digits(input, 2);
        if step == 0 || second >= 60 {
            return (input, false);
        }
        time.set_second(second);
        (&input[step..], true)
    }

    // adjustYear adjusts year according to y.
    // See https://dev.mysql.com/doc/refman/5.7/en/two-digit-years.html
    fn adjust_year(y: u32) -> u32 {
        if y <= 69 {
            return y + 2000;
        } else if (70..=99).contains(&y) {
            return y + 1900;
        }
        y
    }

    fn year_numeric_n_digits<'a>(
        time: &mut Time,
        input: &'a str,
        _: &mut HashMap<String, i64>,
        n: i32,
    ) -> (&'a str, bool) {
        let (mut year, step) = parse_n_digits(input, n);
        if step == 0 {
            return (input, false);
        } else if step <= 2 {
            year = adjust_year(year)
        }
        time.set_year(year);
        (&input[step..], true)
    }

    fn year_numeric_two_digits<'a>(
        time: &mut Time,
        input: &'a str,
        ctx: &mut HashMap<String, i64>,
    ) -> (&'a str, bool) {
        year_numeric_n_digits(time, input, ctx, 2)
    }

    fn year_numeric_four_digits<'a>(
        time: &mut Time,
        input: &'a str,
        ctx: &mut HashMap<String, i64>,
    ) -> (&'a str, bool) {
        year_numeric_n_digits(time, input, ctx, 4)
    }

    fn skip_all_nums<'a>(
        _: &mut Time,
        input: &'a str,
        _: &mut HashMap<String, i64>,
    ) -> (&'a str, bool) {
        let mut step = 0;
        for c in input.chars() {
            if c.is_ascii_digit() {
                step += 1;
            } else {
                break;
            }
        }
        (&input[step..], true)
    }

    fn skip_all_punct<'a>(
        _: &mut Time,
        input: &'a str,
        _: &mut HashMap<String, i64>,
    ) -> (&'a str, bool) {
        let mut step = 0;
        for c in input.chars() {
            if c.is_ascii_punctuation() {
                step += 1;
            } else {
                break;
            }
        }
        (&input[step..], true)
    }

    fn skip_all_alpha<'a>(
        _: &mut Time,
        input: &'a str,
        _: &mut HashMap<String, i64>,
    ) -> (&'a str, bool) {
        let mut step = 0;
        for c in input.chars() {
            if c.is_alphabetic() {
                step += 1;
            } else {
                break;
            }
        }
        (&input[step..], true)
    }

    enum ParseState {
        ParseStateNormal,
        ParseStateFail,
        ParseStateEndOfLine,
    }

    fn parse_sep(input: &str) -> (&str, ParseState) {
        let input = input.trim();
        if input.is_empty() {
            return (input, ParseState::ParseStateEndOfLine);
        }
        if !input.starts_with(':') {
            return (input, ParseState::ParseStateFail);
        }
        let input = (input[1..]).trim();
        if input.is_empty() {
            return (input, ParseState::ParseStateEndOfLine);
        }
        (input, ParseState::ParseStateNormal)
    }

    fn time_12_hour<'a>(
        time: &mut Time,
        input: &'a str,
        _: &mut HashMap<String, i64>,
    ) -> (&'a str, bool) {
        let mut try_parse = |input: &'a str| -> (&'a str, ParseState) {
            // hh:mm:ss AM
            // Note that we should update `t` as soon as possible, or we
            // can not get correct result for incomplete input like "12:13"
            // that is shorter than "hh:mm:ss"
            let (mut hour, step) = parse_n_digits(input, 2);
            if step == 0 || hour > 12 || hour == 0 {
                return (input, ParseState::ParseStateFail);
            }
            // Handle special case: 12:34:56 AM -> 00:34:56
            // For PM, we will add 12 it later
            if hour == 12 {
                hour = 0
            }
            time.set_hour(hour);
            // ":"
            let (input, state) = parse_sep(&input[step..]);
            if let ParseState::ParseStateFail | ParseState::ParseStateEndOfLine = state {
                return (input, state);
            }
            let (minute, step) = parse_n_digits(input, 2);
            if step == 0 || minute > 59 {
                return (input, ParseState::ParseStateFail);
            }
            time.set_minute(minute);
            // ":"
            let (input, state) = parse_sep(&input[step..]);
            if let ParseState::ParseStateFail | ParseState::ParseStateEndOfLine = state {
                return (input, state);
            }
            let (second, step) = parse_n_digits(input, 2);
            if step == 0 || second > 59 {
                return (input, ParseState::ParseStateFail);
            }
            time.set_second(second);

            let input = (input[step..]).trim();
            match input.len() {
                0 => return (input, ParseState::ParseStateEndOfLine), // No "AM"/"PM" suffix
                1 => return (input, ParseState::ParseStateFail),      // some broken char, fail
                _ => {
                    if has_case_insensitive_prefix(input, "AM") {
                        time.set_hour(hour);
                    } else if has_case_insensitive_prefix(input, "PM") {
                        time.set_hour(hour + 12);
                    } else {
                        return (input, ParseState::ParseStateFail);
                    }
                }
            }

            (&input[2..], ParseState::ParseStateNormal)
        };

        let (remain, state) = try_parse(input);
        if let ParseState::ParseStateFail = state {
            return (input, false);
        }
        (remain, true)
    }

    fn time_24_hour<'a>(
        time: &mut Time,
        input: &'a str,
        _: &mut HashMap<String, i64>,
    ) -> (&'a str, bool) {
        let mut try_parse = |input: &'a str| -> (&'a str, ParseState) {
            // hh:mm:ss AM
            // Note that we should update `t` as soon as possible, or we
            // can not get correct result for incomplete input like "12:13"
            // that is shorter than "hh:mm:ss"
            let (hour, step) = parse_n_digits(input, 2);
            if step == 0 || hour > 23 {
                return (input, ParseState::ParseStateFail);
            }
            time.set_hour(hour);
            // ":"
            let (input, state) = parse_sep(&input[step..]);
            if let ParseState::ParseStateFail | ParseState::ParseStateEndOfLine = state {
                return (input, state);
            }
            let (minute, step) = parse_n_digits(input, 2);
            if step == 0 || minute > 59 {
                return (input, ParseState::ParseStateFail);
            }
            time.set_minute(minute);
            // ":"
            let (input, state) = parse_sep(&input[step..]);
            if let ParseState::ParseStateFail | ParseState::ParseStateEndOfLine = state {
                return (input, state);
            }
            let (second, step) = parse_n_digits(input, 2);
            if step == 0 || second > 59 {
                return (input, ParseState::ParseStateFail);
            }
            time.set_second(second);
            (&input[step..], ParseState::ParseStateNormal)
        };

        let (remain, state) = try_parse(input);
        if let ParseState::ParseStateFail = state {
            return (input, false);
        }
        (remain, true)
    }

    fn date_format_parser_mapper(input: &str) -> Option<DateFormatParser<'_>> {
        match input {
            "%b" => Some(abbreviated_month),
            "%c" => Some(month_numeric),
            "%d" => Some(day_of_month_numeric),
            "%e" => Some(day_of_month_numeric),
            "%f" => Some(micro_second),
            "%h" => Some(hour_12_numeric),
            "%H" => Some(hour_24_numeric),
            "%I" => Some(hour_12_numeric),
            "%i" => Some(minute_numeric),
            "%j" => Some(day_of_year_numeric),
            "%k" => Some(hour_24_numeric),
            "%l" => Some(hour_12_numeric),
            "%M" => Some(full_name_month),
            "%m" => Some(month_numeric),
            "%p" => Some(is_am_or_pm),
            "%r" => Some(time_12_hour),
            "%s" => Some(seconds_numeric),
            "%S" => Some(seconds_numeric),
            "%T" => Some(time_24_hour),
            "%Y" => Some(year_numeric_four_digits),
            "%#" => Some(skip_all_nums),
            "%." => Some(skip_all_punct),
            "%@" => Some(skip_all_alpha),
            // Deprecated since MySQL 5.7.5
            "%y" => Some(year_numeric_two_digits),
            _ => None,
        }
    }

    fn get_format_token(format: &str) -> (&str, &str, bool) {
        match format.len() {
            0 => ("", "", true),
            1 => match format.as_bytes()[0] {
                b'%' => ("", "", false),
                _ => ("", format, true),
            },
            _ => match format.as_bytes()[0] {
                b'%' => (&format[..2], &format[2..], true),
                _ => (&format[..1], &format[1..], true),
            },
        }
    }

    fn match_date_with_token<'a>(
        time: &mut Time,
        date: &'a str,
        token: &'a str,
        ctx: &mut HashMap<String, i64>,
    ) -> (&'a str, bool) {
        match date_format_parser_mapper(token) {
            Some(parse) => parse(time, date, ctx),
            None => match date.starts_with(token) {
                true => (&date[token.len()..], true),
                false => (date, false),
            },
        }
    }

    fn str_to_date_impl(
        time: &mut Time,
        date: &str,
        format: &str,
        ctx: &mut HashMap<String, i64>,
    ) -> (bool, bool) {
        let date = date.trim();
        let format = format.trim();
        let (token, format_remain, succ) = get_format_token(format);
        if !succ {
            return (false, false);
        }
        if token.is_empty() {
            if !date.is_empty() {
                return (true, true);
            }
            return (true, false);
        }
        if date.is_empty() {
            ctx.insert(token.into(), 0);
            return (true, false);
        }
        let (date_remain, succ) = match_date_with_token(time, date, token, ctx);
        if !succ {
            return (false, false);
        }
        str_to_date_impl(time, date_remain, format_remain, ctx)
    }

    fn mysql_time_fix(time: &mut Time, ctx: &mut HashMap<String, i64>) -> Result<()> {
        if ctx.contains_key("%p") {
            if ctx.contains_key("%H") || time.hour() == 0 {
                return Err(Error::truncated());
            }
            let am_or_pm = ctx["%p"];
            if time.hour() == 12 {
                match am_or_pm {
                    CONST_FOR_AM => time.set_hour(0),
                    CONST_FOR_PM => time.set_hour(12),
                    _ => {}
                }
                return Ok(());
            }
            if am_or_pm == CONST_FOR_PM {
                time.set_hour(time.hour() + 12);
            }
        } else if ctx.contains_key("%h") {
            let hour = ctx["%h"];
            if hour == 12 {
                time.set_hour(0);
            }
        }
        Ok(())
    }

    pub fn str_to_date(eval_ctx: &mut EvalContext, date: &str, format: &str) -> (bool, Time) {
        let mut ctx = HashMap::new();
        let mut time = Time(0);
        let (succ, warn) = str_to_date_impl(&mut time, date, format, &mut ctx);
        if !succ {
            return (false, time);
        }
        if mysql_time_fix(&mut time, &mut ctx).is_err() {
            return (false, time);
        }

        time.set_time_type(TimeType::DateTime).ok();
        let time_args: TimeArgs = TimeArgs {
            year: time.year(),
            month: time.month(),
            day: time.day(),
            hour: time.hour(),
            minute: time.minute(),
            second: time.second(),
            micro: time.micro(),
            fsp: time.fsp() as i8,
            time_type: time.get_time_type(),
        };
        if time_args.check(eval_ctx).is_none() {
            return (false, time);
        }
        if warn {
            eval_ctx.warnings.append_warning(Error::truncated());
        }
        (true, time)
    }
}

impl Time {
    pub fn parse(
        ctx: &mut EvalContext,
        input: &str,
        time_type: TimeType,
        fsp: i8,
        round: bool,
    ) -> Result<Time> {
        parser::parse(ctx, input, Some(time_type), check_fsp(fsp)?, round)
            .ok_or_else(|| Error::incorrect_datetime_value(input))
    }
    pub fn parse_without_type(
        ctx: &mut EvalContext,
        input: &str,
        fsp: i8,
        round: bool,
    ) -> Result<Time> {
        parser::parse(ctx, input, None, check_fsp(fsp)?, round)
            .ok_or_else(|| Error::incorrect_datetime_value(input))
    }
    pub fn parse_datetime(
        ctx: &mut EvalContext,
        input: &str,
        fsp: i8,
        round: bool,
    ) -> Result<Time> {
        Self::parse(ctx, input, TimeType::DateTime, fsp, round)
    }
    pub fn parse_date(ctx: &mut EvalContext, input: &str) -> Result<Time> {
        Self::parse(ctx, input, TimeType::Date, 0, false)
    }
    pub fn parse_timestamp(
        ctx: &mut EvalContext,
        input: &str,
        fsp: i8,
        round: bool,
    ) -> Result<Time> {
        Self::parse(ctx, input, TimeType::Timestamp, fsp, round)
    }
    pub fn parse_from_i64(
        ctx: &mut EvalContext,
        input: i64,
        time_type: TimeType,
        fsp: i8,
    ) -> Result<Time> {
        parser::parse_from_i64(ctx, input, time_type, check_fsp(fsp)?)
            .ok_or_else(|| Error::incorrect_datetime_value(input))
    }
    pub fn parse_from_real(
        ctx: &mut EvalContext,
        input: &Real,
        time_type: TimeType,
        fsp: i8,
        round: bool,
    ) -> Result<Time> {
        parser::parse_from_float_string(ctx, input.to_string(), time_type, check_fsp(fsp)?, round)
            .ok_or_else(|| Error::incorrect_datetime_value(input))
    }
    pub fn parse_from_decimal(
        ctx: &mut EvalContext,
        input: &Decimal,
        time_type: TimeType,
        fsp: i8,
        round: bool,
    ) -> Result<Time> {
        parser::parse_from_float_string(ctx, input.to_string(), time_type, check_fsp(fsp)?, round)
            .ok_or_else(|| Error::incorrect_datetime_value(input.to_string()))
    }

    pub fn parse_from_i64_default(ctx: &mut EvalContext, input: i64) -> Result<Time> {
        parser::parse_from_i64_default(ctx, input)
            .ok_or_else(|| Error::incorrect_datetime_value(input))
    }
    pub fn parse_from_real_default(ctx: &mut EvalContext, input: &Real) -> Result<Time> {
        parser::parse_from_real_default(ctx, input)
            .ok_or_else(|| Error::incorrect_datetime_value(input))
    }
    pub fn parse_from_decimal_default(ctx: &mut EvalContext, input: &Decimal) -> Result<Time> {
        parser::parse_from_decimal_default(ctx, input)
            .ok_or_else(|| Error::incorrect_datetime_value(input.to_string()))
    }

    pub fn parse_from_string_with_format(
        eval_ctx: &mut EvalContext,
        date: &str,
        format: &str,
    ) -> (bool, Time) {
        date_format_parser::str_to_date(eval_ctx, date, format)
    }
}

fn handle_zero_date(ctx: &mut EvalContext, mut args: TimeArgs) -> Result<Option<TimeArgs>> {
    let sql_mode = ctx.cfg.sql_mode;
    let flags = ctx.cfg.flag;
    let strict_mode = sql_mode.contains(SqlMode::STRICT_ALL_TABLES)
        | sql_mode.contains(SqlMode::STRICT_TRANS_TABLES);
    let no_zero_date = sql_mode.contains(SqlMode::NO_ZERO_DATE);
    let ignore_truncate = flags.contains(Flag::IGNORE_TRUNCATE);

    debug_assert!(args.is_zero());

    if no_zero_date {
        (!strict_mode || ignore_truncate).ok_or(Error::truncated())?;
        ctx.warnings.append_warning(Error::truncated());
        args.clear();
        return Ok(None);
    }
    Ok(Some(args))
}

fn handle_zero_in_date(ctx: &mut EvalContext, mut args: TimeArgs) -> Result<Option<TimeArgs>> {
    let sql_mode = ctx.cfg.sql_mode;
    let flags = ctx.cfg.flag;

    let strict_mode = sql_mode.contains(SqlMode::STRICT_ALL_TABLES)
        | sql_mode.contains(SqlMode::STRICT_TRANS_TABLES);
    let no_zero_in_date = sql_mode.contains(SqlMode::NO_ZERO_IN_DATE);
    let ignore_truncate = flags.contains(Flag::IGNORE_TRUNCATE);

    debug_assert!(args.month == 0 || args.day == 0);

    if no_zero_in_date {
        // If we are in NO_ZERO_IN_DATE + STRICT_MODE, zero-in-date produces and error.
        // Otherwise, we reset the datetime value and check if we enabled NO_ZERO_DATE.
        (!strict_mode || ignore_truncate).ok_or(Error::truncated())?;
        ctx.warnings.append_warning(Error::truncated());
        args.clear();
        return handle_zero_date(ctx, args);
    }

    Ok(Some(args))
}

fn handle_invalid_date(ctx: &mut EvalContext, mut args: TimeArgs) -> Result<Option<TimeArgs>> {
    let sql_mode = ctx.cfg.sql_mode;
    let allow_invalid_date = sql_mode.contains(SqlMode::INVALID_DATES);
    allow_invalid_date.ok_or(Error::truncated())?;
    args.clear();
    handle_zero_date(ctx, args)
}

/// A validator that verify each field for the `Time`
/// NOTE: It's inappropriate to construct `Time` first and then verify it.
/// Because `Time` uses `bitfield`, the range of each field is quite narrow.
/// For example, the size of `month` field is 5 bits. If we get a value 16 for
/// `month` and set it, we will got 0 (16 % 16 == 0) instead 16 which is
/// definitely an invalid value. So we need a larger range for validation.
#[derive(Debug, Clone)]
pub struct TimeArgs {
    year: u32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    micro: u32,
    fsp: i8,
    time_type: TimeType,
}

impl Default for TimeArgs {
    fn default() -> Self {
        TimeArgs {
            year: 0,
            month: 0,
            day: 0,
            hour: 0,
            minute: 0,
            second: 0,
            micro: 0,
            fsp: 0,
            time_type: TimeType::Date,
        }
    }
}

impl TimeArgs {
    fn check(mut self, ctx: &mut EvalContext) -> Option<TimeArgs> {
        self.fsp = check_fsp(self.fsp).ok()? as i8;
        let (fsp, time_type) = (self.fsp, self.time_type);
        match self.time_type {
            TimeType::Date | TimeType::DateTime => self.check_datetime(ctx),
            TimeType::Timestamp => self.check_timestamp(ctx),
        }
        .map(|datetime| datetime.unwrap_or_else(|| TimeArgs::zero(fsp, time_type)))
        .ok()
    }

    pub fn zero(fsp: i8, time_type: TimeType) -> TimeArgs {
        TimeArgs {
            year: 0,
            month: 0,
            day: 0,
            hour: 0,
            minute: 0,
            second: 0,
            micro: 0,
            fsp,
            time_type,
        }
    }

    pub fn clear(&mut self) {
        self.year = 0;
        self.month = 0;
        self.day = 0;
        self.hour = 0;
        self.minute = 0;
        self.second = 0;
        self.micro = 0;
    }

    pub fn is_zero(&self) -> bool {
        self.year == 0
            && self.month == 0
            && self.day == 0
            && self.hour == 0
            && self.minute == 0
            && self.second == 0
            && self.micro == 0
    }

    fn check_date(mut self, ctx: &mut EvalContext) -> Result<Option<Self>> {
        let Self {
            year, month, day, ..
        } = self;

        let is_relaxed = ctx.cfg.sql_mode.contains(SqlMode::INVALID_DATES);

        if self.is_zero() {
            self = try_opt!(handle_zero_date(ctx, self));
        }

        if month == 0 || day == 0 {
            self = try_opt!(handle_zero_in_date(ctx, self));
        }

        if year > 9999 || Time::check_month_and_day(year, month, day, is_relaxed).is_err() {
            return handle_invalid_date(ctx, self);
        }

        Ok(Some(self))
    }

    fn check_datetime(self, ctx: &mut EvalContext) -> Result<Option<Self>> {
        let datetime = try_opt!(self.check_date(ctx));

        let Self {
            hour,
            minute,
            second,
            micro,
            ..
        } = datetime;

        if hour > 23 || minute > 59 || second > 59 || micro > 999999 {
            return handle_invalid_date(ctx, datetime);
        }

        Ok(Some(datetime))
    }

    fn check_timestamp(self, ctx: &mut EvalContext) -> Result<Option<Self>> {
        if self.is_zero() {
            return handle_zero_date(ctx, self);
        }

        let datetime = chrono_datetime(
            &ctx.cfg.tz,
            self.year,
            self.month,
            self.day,
            self.hour,
            self.minute,
            self.second,
            self.micro,
        );

        if datetime.is_err() {
            return handle_invalid_date(ctx, self);
        }

        let ts = datetime.unwrap().timestamp();

        // Out of range
        if !(MIN_TIMESTAMP..=MAX_TIMESTAMP).contains(&ts) {
            return handle_invalid_date(ctx, self);
        }

        Ok(Some(self))
    }
}

// Utility
impl Time {
    pub fn from_slice(
        ctx: &mut EvalContext,
        parts: &[u32],
        time_type: TimeType,
        fsp: u8,
    ) -> Option<Self> {
        let [year, month, day, hour, minute, second, micro]: [u32; 7] = parts.try_into().ok()?;

        Time::new(
            ctx,
            TimeArgs {
                year,
                month,
                day,
                hour,
                minute,
                second,
                micro,
                fsp: fsp as i8,
                time_type,
            },
        )
        .ok()
    }

    /// Construct a `Time` via a number in format: yyyymmddhhmmss
    fn from_aligned_i64(
        ctx: &mut EvalContext,
        input: i64,
        time_type: TimeType,
        fsp: i8,
    ) -> Result<Time> {
        let ymd = (input / 1_000_000) as u32;
        let hms = (input % 1_000_000) as u32;

        let year = ymd / 10_000;
        let md = ymd % 10_000_u32;
        let month = md / 100;
        let day = md % 100;

        let hour = hms / 10_000;
        let ms = hms % 10_000;
        let minute = ms / 100;
        let second = ms % 100;

        Time::new(
            ctx,
            TimeArgs {
                year,
                month,
                day,
                hour,
                minute,
                second,
                micro: 0,
                time_type,
                fsp,
            },
        )
    }

    fn into_array(self) -> [u32; 7] {
        let mut slice = [0; 7];
        slice[0] = self.year();
        slice[1] = self.month();
        slice[2] = self.day();
        slice[3] = self.hour();
        slice[4] = self.minute();
        slice[5] = self.second();
        slice[6] = self.micro();
        slice
    }

    fn try_from_chrono_datetime<T: Datelike + Timelike>(
        ctx: &mut EvalContext,
        datetime: T,
        time_type: TimeType,
        fsp: i8,
    ) -> Result<Self> {
        Time::new(
            ctx,
            TimeArgs {
                year: datetime.year() as u32,
                month: datetime.month(),
                day: datetime.day(),
                hour: datetime.hour(),
                minute: datetime.minute(),
                second: datetime.second(),
                micro: datetime.nanosecond() / 1000,
                fsp,
                time_type,
            },
        )
    }

    fn try_into_chrono_datetime(self, ctx: &mut EvalContext) -> Result<DateTime<Tz>> {
        chrono_datetime(
            &ctx.cfg.tz,
            self.year(),
            self.month(),
            self.day(),
            self.hour(),
            self.minute(),
            self.second(),
            self.micro(),
        )
    }

    fn try_into_chrono_naive_datetime(self) -> Result<NaiveDateTime> {
        chrono_naive_datetime(
            self.year(),
            self.month(),
            self.day(),
            self.hour(),
            self.minute(),
            self.second(),
            self.micro(),
        )
    }

    fn unchecked_new(config: TimeArgs) -> Self {
        let mut time = Time(0);
        let TimeArgs {
            year,
            month,
            day,
            hour,
            minute,
            second,
            micro,
            fsp,
            time_type,
        } = config;
        time.set_year(year);
        time.set_month(month);
        time.set_day(day);
        time.set_hour(hour);
        time.set_minute(minute);
        time.set_second(second);
        time.set_micro(micro);
        time.set_tt(time_type);
        time.set_fsp(fsp as u8);
        time
    }

    fn new(ctx: &mut EvalContext, config: TimeArgs) -> Result<Time> {
        let unchecked_time = Self::unchecked_new(config.clone());
        let mut checked = config
            .check(ctx)
            .ok_or_else(|| Error::incorrect_datetime_value(unchecked_time))?;
        if checked.time_type == TimeType::Date {
            checked.hour = 0;
            checked.minute = 0;
            checked.second = 0;
            checked.micro = 0;
            checked.fsp = 0;
        }
        Ok(Self::unchecked_new(checked))
    }

    fn check_month_and_day(
        year: u32,
        month: u32,
        day: u32,
        allow_invalid_date: bool,
    ) -> Result<()> {
        if month > 12 || day > 31 {
            return Err(Error::truncated());
        }

        if allow_invalid_date {
            return Ok(());
        }

        if day > last_day_of_month(year, month) {
            return Err(Error::truncated());
        }

        Ok(())
    }

    pub fn is_zero(mut self) -> bool {
        self.set_fsp_tt(0);
        self.0 == 0
    }

    pub fn zero(ctx: &mut EvalContext, fsp: i8, time_type: TimeType) -> Result<Self> {
        Time::new(ctx, TimeArgs::zero(fsp, time_type))
    }

    #[inline]
    pub fn is_leap_year(self) -> bool {
        is_leap_year(self.year())
    }

    #[inline]
    pub fn last_day_of_month(self) -> u32 {
        last_day_of_month(self.year(), self.month())
    }

    pub fn last_date_of_month(mut self) -> Option<Self> {
        if self.invalid_zero() {
            return None;
        }
        self.set_day(self.last_day_of_month());
        self.set_hour(0);
        self.set_minute(0);
        self.set_second(0);
        self.set_micro(0);
        Some(self)
    }

    #[inline]
    pub fn fsp(self) -> u8 {
        let fsp = self.get_fsp_tt() >> 1;
        match self.get_time_type() {
            TimeType::Date => 0,
            _ => fsp,
        }
    }

    #[inline]
    pub fn set_fsp(&mut self, mut fsp: u8) {
        if self.get_time_type() == TimeType::Date {
            return;
        }
        if fsp > super::MAX_FSP as u8 {
            fsp = super::MAX_FSP as u8;
        }
        self.set_fsp_tt((fsp << 1) | (self.get_fsp_tt() & 1));
    }

    #[inline]
    pub fn maximize_fsp(&mut self) {
        self.set_fsp(super::MAX_FSP as u8);
    }

    #[inline]
    pub fn minimize_fsp(&mut self) {
        self.set_fsp(super::MIN_FSP as u8);
    }

    #[inline]
    pub fn get_time_type(self) -> TimeType {
        let ft = self.get_fsp_tt();

        if ft >> 1 == 0b111 {
            TimeType::Date
        } else if ft & 1 == 0 {
            TimeType::DateTime
        } else {
            TimeType::Timestamp
        }
    }

    #[inline]
    pub fn set_time_type(&mut self, time_type: TimeType) -> Result<()> {
        if self.get_time_type() != time_type && time_type == TimeType::Date {
            self.set_hour(0);
            self.set_minute(0);
            self.set_second(0);
            self.set_micro(0);
            self.set_fsp(0);
        }
        if self.get_time_type() != time_type && time_type == TimeType::Timestamp {
            return Err(box_err!("can not convert datetime/date to timestamp"));
        }
        self.set_tt(time_type);
        Ok(())
    }

    #[inline]
    fn set_tt(&mut self, time_type: TimeType) {
        let mut ft = self.get_fsp_tt();
        if ft == 0b1110 && time_type != TimeType::Date {
            ft = 0;
        }
        let mask = match time_type {
            TimeType::Date => 0b1110,
            TimeType::DateTime => ft & 0b1110,
            TimeType::Timestamp => ft | 1,
        };
        self.set_fsp_tt(mask);
    }

    pub fn from_packed_u64(
        ctx: &mut EvalContext,
        value: u64,
        time_type: TimeType,
        fsp: i8,
    ) -> Result<Time> {
        if value == 0 {
            return Time::new(ctx, TimeArgs::zero(fsp, time_type));
        }

        let fsp = check_fsp(fsp)?;
        let ymdhms = value >> 24;
        let ymd = ymdhms >> 17;
        let ym = ymd >> 5;
        let hms = ymdhms & ((1 << 17) - 1);

        let day = (ymd & ((1 << 5) - 1)) as u32;
        let month = (ym % 13) as u32;
        let year = (ym / 13) as u32;
        let second = (hms & ((1 << 6) - 1)) as u32;
        let minute = ((hms >> 6) & ((1 << 6) - 1)) as u32;
        let hour = (hms >> 12) as u32;
        let micro = (value & ((1 << 24) - 1)) as u32;

        if time_type == TimeType::Timestamp {
            let utc = chrono_datetime(&Utc, year, month, day, hour, minute, second, micro)?;
            let timestamp = ctx.cfg.tz.from_utc_datetime(&utc.naive_utc());
            Time::try_from_chrono_datetime(ctx, timestamp.naive_local(), time_type, fsp as i8)
        } else {
            Ok(Time::unchecked_new(TimeArgs {
                year,
                month,
                day,
                hour,
                minute,
                second,
                micro,
                fsp: fsp as i8,
                time_type,
            }))
        }
    }

    pub fn to_packed_u64(mut self, ctx: &mut EvalContext) -> Result<u64> {
        if self.is_zero() {
            return Ok(0);
        }

        if self.get_time_type() == TimeType::Timestamp {
            let ts = self.try_into_chrono_datetime(ctx)?;
            self = Time::try_from_chrono_datetime(
                ctx,
                ts.naive_utc(),
                self.get_time_type(),
                self.fsp() as i8,
            )?;
        }

        let ymd =
            ((u64::from(self.year()) * 13 + u64::from(self.month())) << 5) | u64::from(self.day());
        let hms = (u64::from(self.hour()) << 12)
            | (u64::from(self.minute()) << 6)
            | u64::from(self.second());

        Ok((((ymd << 17) | hms) << 24) | u64::from(self.micro()))
    }

    pub fn from_duration(
        ctx: &mut EvalContext,
        duration: Duration,
        time_type: TimeType,
    ) -> Result<Self> {
        let dur = chrono::Duration::nanoseconds(duration.to_nanos());

        let time = if unlikely(ctx.cfg.is_test) {
            Utc.ymd(2020, 2, 2).and_hms(0, 0, 0).checked_add_signed(dur)
        } else {
            Utc::today().and_hms(0, 0, 0).checked_add_signed(dur)
        };

        let time = time.ok_or::<Error>(box_err!("parse from duration {} overflows", duration))?;

        Time::try_from_chrono_datetime(ctx, time, time_type, duration.fsp() as i8)
    }

    pub fn from_local_time(ctx: &mut EvalContext, time_type: TimeType, fsp: i8) -> Result<Time> {
        let fsp = check_fsp(fsp)?;
        let utc = Local::now();
        let timestamp = ctx.cfg.tz.from_utc_datetime(&utc.naive_utc());
        Time::try_from_chrono_datetime(ctx, timestamp.naive_local(), time_type, fsp as i8)
    }

    pub fn from_unixtime(
        ctx: &mut EvalContext,
        seconds: i64,
        nanos: u32,
        time_type: TimeType,
        fsp: i8,
    ) -> Result<Self> {
        let timestamp = Utc.timestamp(seconds, nanos);
        let timestamp = ctx.cfg.tz.from_utc_datetime(&timestamp.naive_utc());
        let timestamp = timestamp.round_subsecs(fsp as u16);
        Time::try_from_chrono_datetime(ctx, timestamp.naive_local(), time_type, fsp)
    }

    pub fn from_year(
        ctx: &mut EvalContext,
        year: u32,
        fsp: i8,
        time_type: TimeType,
    ) -> Result<Self> {
        Time::new(
            ctx,
            TimeArgs {
                year,
                month: 0,
                day: 0,
                hour: 0,
                minute: 0,
                second: 0,
                micro: 0,
                fsp,
                time_type,
            },
        )
    }

    pub fn round_frac(mut self, ctx: &mut EvalContext, fsp: i8) -> Result<Self> {
        let time_type = self.get_time_type();
        if time_type == TimeType::Date || self.is_zero() {
            return Ok(self);
        }

        let fsp = check_fsp(fsp)?;
        if fsp > self.fsp() {
            self.set_fsp(fsp);
            return Ok(self);
        }
        let (carry, frac) = round_frac(self.micro(), fsp);
        let mut slice = self.into_array();
        slice[6] = frac;

        // If we have cases like:
        //   1. 2012-0-1  23:59:59.999      (fsp: 2)
        //   2. 2012-4-31 23:59:59.999      (fsp: 2)
        // 0000-00-00.00 is expected.
        if carry && round_components(&mut slice).is_none() {
            return Time::new(ctx, TimeArgs::zero(fsp as i8, time_type));
        }

        Time::from_slice(ctx, &slice, time_type, fsp)
            .ok_or_else(|| Error::incorrect_datetime_value(self))
    }

    pub fn normalized(self, ctx: &mut EvalContext) -> Result<Self> {
        if self.get_time_type() == TimeType::Timestamp {
            return Ok(self);
        }

        if self.day() > self.last_day_of_month() || self.month() == 0 || self.day() == 0 {
            let date = if self.month() == 0 {
                (self.year() >= 1).ok_or(Error::incorrect_datetime_value(self))?;
                NaiveDate::from_ymd(self.year() as i32 - 1, 12, 1)
            } else {
                NaiveDate::from_ymd(self.year() as i32, self.month(), 1)
            } + chrono::Duration::days(i64::from(self.day()) - 1);
            let datetime = NaiveDateTime::new(
                date,
                NaiveTime::from_hms_micro(self.hour(), self.minute(), self.second(), self.micro()),
            );
            return Time::try_from_chrono_datetime(
                ctx,
                datetime,
                self.get_time_type(),
                self.fsp() as i8,
            );
        }

        Ok(self)
    }

    pub fn checked_add(self, ctx: &mut EvalContext, rhs: Duration) -> Option<Time> {
        let normalized = self.normalized(ctx).ok()?;
        let duration = chrono::Duration::nanoseconds(rhs.to_nanos());
        if self.get_time_type() == TimeType::Timestamp {
            let datetime = normalized
                .try_into_chrono_datetime(ctx)
                .ok()
                .and_then(|datetime| datetime.checked_add_signed(duration))?;
            Time::try_from_chrono_datetime(ctx, datetime, TimeType::Timestamp, self.fsp() as i8)
        } else {
            let naive = normalized
                .try_into_chrono_naive_datetime()
                .ok()
                .and_then(|datetime| datetime.checked_add_signed(duration))?;
            Time::try_from_chrono_datetime(ctx, naive, TimeType::Timestamp, self.fsp() as i8)
        }
        .ok()
    }

    pub fn checked_sub(self, ctx: &mut EvalContext, rhs: Duration) -> Option<Time> {
        let normalized = self.normalized(ctx).ok()?;
        let duration = chrono::Duration::nanoseconds(rhs.to_nanos());
        if self.get_time_type() == TimeType::Timestamp {
            let datetime = normalized
                .try_into_chrono_datetime(ctx)
                .ok()
                .and_then(|datetime| datetime.checked_sub_signed(duration))?;
            Time::try_from_chrono_datetime(ctx, datetime, TimeType::Timestamp, self.fsp() as i8)
        } else {
            let naive = normalized
                .try_into_chrono_naive_datetime()
                .ok()
                .and_then(|datetime| datetime.checked_sub_signed(duration))?;
            Time::try_from_chrono_datetime(ctx, naive, TimeType::Timestamp, self.fsp() as i8)
        }
        .ok()
    }

    pub fn add_sec_nanos(self, ctx: &mut EvalContext, secs: i64, nanos: i64) -> Result<Time> {
        // https://github.com/pingcap/tidb/blob/3ee87658d283441408e6132c80c0c00019d2fff1/pkg/types/core_time.go#L283
        const MAX_SECS: i64 = 10000 * 365 * SECS_PER_DAY;
        const MIN_SECS: i64 = -MAX_SECS;
        if !(MIN_SECS..=MAX_SECS).contains(&secs) {
            return Err(Error::datetime_function_overflow());
        }
        let sec_dur = chrono::Duration::seconds(secs);
        let duration = sec_dur
            .checked_add(&chrono::Duration::nanoseconds(nanos))
            .ok_or_else(|| Error::datetime_function_overflow())?;
        let time_type = self.get_time_type();
        let fsp = self.fsp() as i8;
        let mut new_time = if time_type == TimeType::Timestamp {
            let datetime = self
                .try_into_chrono_datetime(ctx)?
                .checked_add_signed(duration)
                .ok_or_else(|| Error::datetime_function_overflow())?;
            Time::try_from_chrono_datetime(ctx, datetime, TimeType::Timestamp, fsp)?
        } else {
            let naive = self
                .try_into_chrono_naive_datetime()?
                .checked_add_signed(duration)
                .ok_or_else(|| Error::datetime_function_overflow())?;
            Time::try_from_chrono_datetime(ctx, naive, time_type, fsp)?
        };

        if new_time.year() == 0 {
            // Special handling for year 0
            new_time.set_month(0);
            new_time.set_day(0);
        }
        Ok(new_time)
    }

    pub fn add_months(&mut self, months: i64) -> Result<()> {
        // Get the current year, month, and day
        let mut current_year = self.get_year() as i64;
        // Months are 1-based, subtract 1 to make it 0-based
        let mut current_month = self.get_month() as i64 - 1;
        let current_day = self.get_day();

        // Calculate new month and year
        current_month += months;
        if current_month >= 0 {
            current_year += current_month / 12;
            current_month %= 12;
        } else {
            let mut year_decrease = (-current_month) / 12;
            if (-current_month) % 12 != 0 {
                year_decrease += 1;
            }
            current_month += year_decrease * 12;
            current_year -= year_decrease;
        }

        // Overflow check: year must be between 0 and 9999
        if !(0..=9999).contains(&current_year) {
            return Err(Error::datetime_function_overflow());
        }
        if current_year == 0 {
            // Special handling for year 0
            self.set_year(0);
            self.set_month(0);
            self.set_day(0);
            return Ok(());
        }

        // Update the year
        self.set_year(current_year as u32);

        // Determine the maximum number of days in the current month
        const DAY_NUM_IN_MONTH: [i32; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        const DAY_NUM_IN_MONTH_LEAP_YEAR: [i32; 12] =
            [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

        let max_day = if is_leap_year(current_year as u32) {
            DAY_NUM_IN_MONTH_LEAP_YEAR[current_month as usize]
        } else {
            DAY_NUM_IN_MONTH[current_month as usize]
        };

        // Update the month
        self.set_month((current_month + 1) as u32); // Convert month back to 1-based

        // Update the day, ensuring it doesn't exceed the maximum number of days in the
        // month
        self.set_day(std::cmp::min(current_day, max_day as u32));

        Ok(())
    }

    pub fn date_diff(mut self, mut other: Self) -> Option<i64> {
        if self.invalid_zero() || other.invalid_zero() {
            return None;
        }
        self.set_time_type(TimeType::Date).ok()?;
        other.set_time_type(TimeType::Date).ok()?;

        let lhs = self.try_into_chrono_naive_datetime().ok()?;
        let rhs = other.try_into_chrono_naive_datetime().ok()?;
        Some(lhs.signed_duration_since(rhs).num_days())
    }

    pub fn ordinal(self) -> i32 {
        if self.month() == 0 {
            return self.day() as i32 - 32;
        }
        ((1..self.month()).fold(0, |acc, month| acc + last_day_of_month(self.year(), month))
            + self.day()) as i32
    }

    pub fn weekday(self) -> Weekday {
        let date = if self.month() == 0 {
            NaiveDate::from_ymd(self.year() as i32 - 1, 12, 1)
        } else {
            NaiveDate::from_ymd(self.year() as i32, self.month(), 1)
        } + chrono::Duration::days(i64::from(self.day()) - 1);
        date.weekday()
    }

    fn write_date_format_segment(self, b: char, output: &mut String) -> Result<()> {
        match b {
            'b' => {
                let month = self.month();
                if month == 0 {
                    return Err(box_err!("invalid time format"));
                } else {
                    output.push_str(MONTH_NAMES_ABBR[(month - 1) as usize]);
                }
            }
            'M' => {
                let month = self.month();
                if month == 0 {
                    return Err(box_err!("invalid time format"));
                } else {
                    output.push_str(MONTH_NAMES[(month - 1) as usize]);
                }
            }
            'm' => {
                write!(output, "{:02}", self.month()).unwrap();
            }
            'c' => {
                write!(output, "{}", self.month()).unwrap();
            }
            'D' => {
                write!(output, "{}{}", self.day(), self.abbr_day_of_month()).unwrap();
            }
            'd' => {
                write!(output, "{:02}", self.day()).unwrap();
            }
            'e' => {
                write!(output, "{}", self.day()).unwrap();
            }
            'j' => {
                write!(output, "{:03}", self.days()).unwrap();
            }
            'H' => {
                write!(output, "{:02}", self.hour()).unwrap();
            }
            'k' => {
                write!(output, "{}", self.hour()).unwrap();
            }
            'h' | 'I' => {
                let t = self.hour();
                if t == 0 || t == 12 {
                    output.push_str("12");
                } else {
                    write!(output, "{:02}", t % 12).unwrap();
                }
            }
            'l' => {
                let t = self.hour();
                if t == 0 || t == 12 {
                    output.push_str("12");
                } else {
                    write!(output, "{}", t % 12).unwrap();
                }
            }
            'i' => {
                write!(output, "{:02}", self.minute()).unwrap();
            }
            'p' => {
                let hour = self.hour();
                if (hour / 12) % 2 == 0 {
                    output.push_str("AM")
                } else {
                    output.push_str("PM")
                }
            }
            'r' => {
                let h = self.hour();
                if h == 0 {
                    write!(
                        output,
                        "{:02}:{:02}:{:02} AM",
                        12,
                        self.minute(),
                        self.second()
                    )
                    .unwrap();
                } else if h == 12 {
                    write!(
                        output,
                        "{:02}:{:02}:{:02} PM",
                        12,
                        self.minute(),
                        self.second()
                    )
                    .unwrap();
                } else if h < 12 {
                    write!(
                        output,
                        "{:02}:{:02}:{:02} AM",
                        h,
                        self.minute(),
                        self.second()
                    )
                    .unwrap();
                } else {
                    write!(
                        output,
                        "{:02}:{:02}:{:02} PM",
                        h - 12,
                        self.minute(),
                        self.second()
                    )
                    .unwrap();
                }
            }
            'T' => {
                write!(
                    output,
                    "{:02}:{:02}:{:02}",
                    self.hour(),
                    self.minute(),
                    self.second()
                )
                .unwrap();
            }
            'S' | 's' => {
                write!(output, "{:02}", self.second()).unwrap();
            }
            'f' => {
                write!(output, "{:06}", self.micro()).unwrap();
            }
            'U' => {
                let w = self.week(WeekMode::from_bits_truncate(0));
                write!(output, "{:02}", w).unwrap();
            }
            'u' => {
                let w = self.week(WeekMode::from_bits_truncate(1));
                write!(output, "{:02}", w).unwrap();
            }
            'V' => {
                let w = self.week(WeekMode::from_bits_truncate(2));
                write!(output, "{:02}", w).unwrap();
            }
            'v' => {
                let (_, w) = self.year_week(WeekMode::from_bits_truncate(3));
                write!(output, "{:02}", w).unwrap();
            }
            'a' => {
                output.push_str(self.weekday().name_abbr());
            }
            'W' => {
                output.push_str(self.weekday().name());
            }
            'w' => {
                write!(output, "{}", self.weekday().num_days_from_sunday()).unwrap();
            }
            'X' => {
                let (year, _) = self.year_week(WeekMode::from_bits_truncate(2));
                if year < 0 {
                    write!(output, "{}", u32::MAX).unwrap();
                } else {
                    write!(output, "{:04}", year).unwrap();
                }
            }
            'x' => {
                let (year, _) = self.year_week(WeekMode::from_bits_truncate(3));
                if year < 0 {
                    write!(output, "{}", u32::MAX).unwrap();
                } else {
                    write!(output, "{:04}", year).unwrap();
                }
            }
            'Y' => {
                write!(output, "{:04}", self.year()).unwrap();
            }
            'y' => {
                write!(output, "{:02}", self.year() % 100).unwrap();
            }
            _ => output.push(b),
        }
        Ok(())
    }

    pub fn date_format(self, layout: &str) -> Result<String> {
        let mut ret = String::new();
        let mut pattern_match = false;
        for b in layout.chars() {
            if pattern_match {
                self.write_date_format_segment(b, &mut ret)?;
                pattern_match = false;
                continue;
            }
            if b == '%' {
                pattern_match = true;
            } else {
                ret.push(b);
            }
        }
        Ok(ret)
    }

    /// Converts a `DateTime` to printable string representation
    #[inline]
    pub fn to_numeric_string(self) -> String {
        let mut buffer = String::with_capacity(15);
        write!(buffer, "{}", self.date_format("%Y%m%d").unwrap()).unwrap();
        if self.get_time_type() != TimeType::Date {
            write!(buffer, "{}", self.date_format("%H%i%S").unwrap()).unwrap();
        }
        let fsp = usize::from(self.fsp());
        if fsp > 0 {
            write!(
                buffer,
                ".{:0width$}",
                self.micro() / TEN_POW[MICRO_WIDTH - fsp],
                width = fsp
            )
            .unwrap();
        }
        buffer
    }

    pub fn parse_fsp(s: &str) -> i8 {
        s.rfind('.').map_or(super::DEFAULT_FSP, |idx| {
            std::cmp::min((s.len() - idx - 1) as i8, super::MAX_FSP)
        })
    }

    pub fn invalid_zero(self) -> bool {
        self.month() == 0 || self.day() == 0
    }

    pub fn from_days(ctx: &mut EvalContext, daynr: u32) -> Result<Self> {
        let (year, month, day) = Time::get_date_from_daynr(daynr);
        let time_args = TimeArgs {
            year,
            month,
            day,
            ..Default::default()
        };
        Time::new(ctx, time_args)
    }

    // Changes a daynr to year, month and day, daynr 0 is returned as date 00.00.00
    #[inline]
    fn get_date_from_daynr(daynr: u32) -> (u32, u32, u32) {
        if daynr <= 365 || daynr >= 3_652_425 {
            return (0, 0, 0);
        }

        let mut year = daynr * 100 / 36525;
        let temp = (((year - 1) / 100 + 1) * 3) / 4;
        let mut day_of_year = daynr - year * 365 - (year - 1) / 4 + temp;

        let mut days_in_year = if is_leap_year(year) { 366 } else { 365 };
        while day_of_year > days_in_year {
            day_of_year -= days_in_year;
            year += 1;
            days_in_year = if is_leap_year(year) { 366 } else { 365 };
        }

        let mut month = 1;
        for each_month in 1..=12 {
            let last_day_of_month = last_day_of_month(year, each_month);
            if day_of_year <= last_day_of_month {
                break;
            }
            month += 1;
            day_of_year -= last_day_of_month;
        }

        let day = day_of_year;

        (year, month, day)
    }

    /// Calculates days since 0000-00-00.
    pub fn get_daynr(year: u32, month: u32, day: u32) -> u32 {
        if year == 0 && month == 0 {
            return 0;
        }

        let mut delsum = 365 * year as i64 + 31 * (month as i64 - 1) + day as i64;
        let mut year = year as i64;

        if month <= 2 {
            year -= 1;
        } else {
            delsum -= (month as i64 * 4 + 23) / 10;
        }

        let temp = ((year / 100 + 1) * 3) / 4;
        (delsum + year / 4 - temp) as u32
    }

    fn timestamp_diff_internal(&self, other: &Self) -> (i64, i64, bool) {
        let days1 = Time::get_daynr(self.year(), self.month(), self.day());
        let days2 = Time::get_daynr(other.year(), other.month(), other.day());

        let days_diff = days1 as i64 - days2 as i64;

        let diff = (days_diff * SECS_PER_DAY
            + self.hour() as i64 * SECS_PER_HOUR
            + self.minute() as i64 * SECS_PER_MINUTE
            + self.second() as i64
            - (other.hour() as i64 * SECS_PER_HOUR
                + other.minute() as i64 * SECS_PER_MINUTE
                + other.second() as i64))
            * MICROS_PER_SEC
            + self.micro() as i64
            - other.micro() as i64;

        let diff_abs = diff.abs();
        (
            diff_abs / MICROS_PER_SEC,
            diff_abs % MICROS_PER_SEC,
            diff < 0,
        )
    }

    pub fn timestamp_diff(&self, other: &Self, unit: IntervalUnit) -> Result<i64> {
        let (seconds, microseconds, neg) = other.timestamp_diff_internal(self);

        let mut months = 0;

        if matches!(
            unit,
            IntervalUnit::Year | IntervalUnit::Quarter | IntervalUnit::Month
        ) {
            let (year_start, year_end, month_start, month_end, day_start, day_end);
            let (second_start, second_end, microsecond_start, microsecond_end);

            // Swap values if the difference is negative
            if neg {
                year_start = other.year();
                year_end = self.year();
                month_start = other.month();
                month_end = self.month();
                day_start = other.day();
                day_end = self.day();
                second_start = other.hour() * SECS_PER_HOUR as u32
                    + other.minute() * SECS_PER_MINUTE as u32
                    + other.second();
                second_end = self.hour() * SECS_PER_HOUR as u32
                    + self.minute() * SECS_PER_MINUTE as u32
                    + self.second();
                microsecond_start = other.micro();
                microsecond_end = self.micro();
            } else {
                year_start = self.year();
                year_end = other.year();
                month_start = self.month();
                month_end = other.month();
                day_start = self.day();
                day_end = other.day();
                second_start = self.hour() * SECS_PER_HOUR as u32
                    + self.minute() * SECS_PER_MINUTE as u32
                    + self.second();
                second_end = other.hour() * SECS_PER_HOUR as u32
                    + other.minute() * SECS_PER_MINUTE as u32
                    + other.second();
                microsecond_start = self.micro();
                microsecond_end = other.micro();
            }

            // Calculate the number of full years
            let mut years = year_end - year_start;
            if month_end < month_start || (month_end == month_start && day_end < day_start) {
                years -= 1;
            }

            // Calculate the total number of months
            months = 12 * years as i64;
            if month_end < month_start || (month_end == month_start && day_end < day_start) {
                months += 12 - (month_start as i64 - month_end as i64);
            } else {
                months += month_end as i64 - month_start as i64;
            }

            // Adjust if the day or time within the month is earlier
            if day_end < day_start
                || (day_end == day_start
                    && (second_end < second_start
                        || (second_end == second_start && microsecond_end < microsecond_start)))
            {
                months -= 1;
            }
        }

        let neg_v = if neg { -1 } else { 1 };

        Ok(match unit {
            IntervalUnit::Year => (months / 12) * neg_v,
            IntervalUnit::Quarter => (months / 3) * neg_v,
            IntervalUnit::Month => months * neg_v,
            IntervalUnit::Week => (seconds / SECS_PER_DAY / 7) * neg_v,
            IntervalUnit::Day => (seconds / SECS_PER_DAY) * neg_v,
            IntervalUnit::Hour => (seconds / SECS_PER_HOUR) * neg_v,
            IntervalUnit::Minute => (seconds / SECS_PER_MINUTE) * neg_v,
            IntervalUnit::Second => seconds * neg_v,
            IntervalUnit::Microsecond => (seconds * MICROS_PER_SEC + microseconds) * neg_v,
            _ => return Err(box_err!("wrong unit {:?} for timestamp_diff", unit)),
        })
    }
}

impl ConvertTo<f64> for Time {
    /// This function should not return err,
    /// if it return err, then the err is because of bug.
    #[inline]
    fn convert(&self, _: &mut EvalContext) -> Result<f64> {
        if self.is_zero() {
            return Ok(0f64);
        }
        let r = self.to_numeric_string().parse::<f64>();
        debug_assert!(r.is_ok());
        Ok(r?)
    }
}

impl ConvertTo<Decimal> for Time {
    // Port from TiDB's Time::ToNumber
    #[inline]
    fn convert(&self, _: &mut EvalContext) -> Result<Decimal> {
        if self.is_zero() {
            return Ok(0.into());
        }

        self.to_numeric_string().parse()
    }
}

impl ConvertTo<Duration> for Time {
    /// Port from TiDB's Time::ConvertToDuration
    #[inline]
    fn convert(&self, _: &mut EvalContext) -> Result<Duration> {
        if self.is_zero() {
            return Ok(Duration::zero());
        }
        let seconds = i64::from(self.hour() * 3600 + self.minute() * 60 + self.second());
        // `microsecond` returns the number of microseconds since the whole non-leap
        // second. Such as for 2019-09-22 07:21:22.670936103 UTC,
        // it will return 670936103.
        let microsecond = i64::from(self.micro());
        Duration::from_micros(seconds * 1_000_000 + microsecond, self.fsp() as i8)
    }
}

impl PartialEq for Time {
    fn eq(&self, other: &Self) -> bool {
        let mut a = *self;
        let mut b = *other;
        a.set_fsp_tt(0);
        b.set_fsp_tt(0);
        a.0 == b.0
    }
}

impl Eq for Time {}

impl PartialOrd for Time {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Time {
    fn cmp(&self, right: &Self) -> Ordering {
        let mut a = *self;
        let mut b = *right;
        a.set_fsp_tt(0);
        b.set_fsp_tt(0);
        a.0.cmp(&b.0)
    }
}

impl Hash for Time {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let mut a = *self;
        a.set_fsp_tt(0);
        a.0.hash(state);
    }
}

impl std::fmt::Display for Time {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:04}-{:02}-{:02}",
            self.year(),
            self.month(),
            self.day()
        )?;

        if self.get_time_type() != TimeType::Date {
            write!(f, " ")?;
            write!(
                f,
                "{:02}:{:02}:{:02}",
                self.hour(),
                self.minute(),
                self.second()
            )?;
            let fsp = usize::from(self.fsp());
            if fsp > 0 {
                write!(
                    f,
                    ".{:0width$}",
                    self.micro() / TEN_POW[MICRO_WIDTH - fsp],
                    width = fsp
                )?;
            }
        }
        Ok(())
    }
}

impl std::fmt::Debug for Time {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}({:?}: {})({:b})",
            self.year(),
            self.month(),
            self.day(),
            self.hour(),
            self.minute(),
            self.second(),
            self.micro(),
            self.get_time_type(),
            self.fsp(),
            self.0
        )
    }
}

impl<T: BufferWriter> TimeEncoder for T {}

/// Time Encoder for Chunk format
pub trait TimeEncoder: NumberEncoder {
    #[inline]
    fn write_time(&mut self, v: Time) -> Result<()> {
        Ok(self.write_u64_le(v.0)?)
    }
}

pub trait TimeDatumPayloadChunkEncoder: TimeEncoder {
    #[inline]
    fn write_time_to_chunk_by_datum_payload_int(
        &mut self,
        mut src_payload: &[u8],
        ctx: &mut EvalContext,
        field_type: &FieldType,
    ) -> Result<()> {
        let time = src_payload.read_time_int(ctx, field_type)?;
        self.write_time(time)
    }

    #[inline]
    fn write_time_to_chunk_by_datum_payload_varint(
        &mut self,
        mut src_payload: &[u8],
        ctx: &mut EvalContext,
        field_type: &FieldType,
    ) -> Result<()> {
        let time = src_payload.read_time_varint(ctx, field_type)?;
        self.write_time(time)
    }
}

impl<T: BufferWriter> TimeDatumPayloadChunkEncoder for T {}

pub trait TimeDecoder: NumberDecoder {
    #[inline]
    fn read_time_int(&mut self, ctx: &mut EvalContext, field_type: &FieldType) -> Result<Time> {
        let v = self.read_u64()?;
        let fsp = field_type.as_accessor().decimal() as i8;
        let time_type = field_type.as_accessor().tp().try_into()?;
        Time::from_packed_u64(ctx, v, time_type, fsp)
    }

    #[inline]
    fn read_time_varint(&mut self, ctx: &mut EvalContext, field_type: &FieldType) -> Result<Time> {
        let v = self.read_var_u64()?;
        let fsp = field_type.as_accessor().decimal() as i8;
        let time_type = field_type.as_accessor().tp().try_into()?;
        Time::from_packed_u64(ctx, v, time_type, fsp)
    }

    #[inline]
    fn read_time_from_chunk(&mut self) -> Result<Time> {
        let t = self.read_u64_le()?;
        Ok(Time(t))
    }
}

impl<T: BufferReader> TimeDecoder for T {}

impl crate::codec::data_type::AsMySqlBool for Time {
    #[inline]
    fn as_mysql_bool(&self, _context: &mut crate::expr::EvalContext) -> crate::codec::Result<bool> {
        Ok(!self.is_zero())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{
        codec::mysql::{MAX_FSP, UNSPECIFIED_FSP, duration::*},
        expr::EvalConfig,
    };

    #[derive(Debug, Default)]
    struct TimeEnv {
        strict_mode: bool,
        no_zero_in_date: bool,
        no_zero_date: bool,
        allow_invalid_date: bool,
        ignore_truncate: bool,
        time_zone: Option<Tz>,
    }

    impl From<TimeEnv> for EvalContext {
        fn from(config: TimeEnv) -> EvalContext {
            let mut eval_config = EvalConfig::new();
            let mut sql_mode = SqlMode::empty();
            let mut flags = Flag::empty();

            if config.strict_mode {
                sql_mode |= SqlMode::STRICT_ALL_TABLES;
            }
            if config.allow_invalid_date {
                sql_mode |= SqlMode::INVALID_DATES;
            }
            if config.no_zero_date {
                sql_mode |= SqlMode::NO_ZERO_DATE;
            }
            if config.no_zero_in_date {
                sql_mode |= SqlMode::NO_ZERO_IN_DATE;
            }

            if config.ignore_truncate {
                flags |= Flag::IGNORE_TRUNCATE;
            }

            eval_config.set_sql_mode(sql_mode).set_flag(flags).tz =
                config.time_zone.unwrap_or_else(Tz::utc);

            EvalContext::new(Arc::new(eval_config))
        }
    }

    #[test]
    fn test_parse_from_i64() -> Result<()> {
        let cases = vec![
            ("0000-00-00 00:00:00", 0),
            ("2000-01-01 00:00:00", 101),
            ("2045-00-00 00:00:00", 450_000),
            ("2059-12-31 00:00:00", 591_231),
            ("1970-01-01 00:00:00", 700_101),
            ("1999-12-31 00:00:00", 991_231),
            ("1000-01-00 00:00:00", 10_000_100),
            ("2000-01-01 00:00:00", 101_000_000),
            ("2069-12-31 23:59:59", 691_231_235_959),
            ("1970-01-01 00:00:00", 700_101_000_000),
            ("1999-12-31 23:59:59", 991_231_235_959),
            ("0100-00-00 00:00:00", 1_000_000_000_000),
            ("1000-01-01 00:00:00", 10_000_101_000_000),
            ("1999-01-01 00:00:00", 19_990_101_000_000),
        ];
        let mut ctx = EvalContext::default();
        for (expected, input) in cases {
            let actual = Time::parse_from_i64(&mut ctx, input, TimeType::DateTime, 0)?;
            assert_eq!(actual.to_string(), expected);
        }

        let should_fail = vec![-1111, 1, 100, 700_100, 100_000_000, 100_000_101_000_000];
        for case in should_fail {
            Time::parse_from_i64(&mut ctx, case, TimeType::DateTime, 0).unwrap_err();
        }
        Ok(())
    }

    #[test]
    fn test_parse_from_i64_default() -> Result<()> {
        let cases = vec![
            ("0000-00-00", 0),
            ("2000-01-01", 101),
            ("2045-00-00", 450_000),
            ("2059-12-31", 591_231),
            ("1970-01-01", 700_101),
            ("1999-12-31", 991_231),
            ("1000-01-00", 10_000_100),
            ("2000-01-01 00:00:00", 101_000_000),
            ("2069-12-31 23:59:59", 691_231_235_959),
            ("1970-01-01 00:00:00", 700_101_000_000),
            ("1999-12-31 23:59:59", 991_231_235_959),
            ("0100-00-00 00:00:00", 1_000_000_000_000),
            ("1000-01-01 00:00:00", 10_000_101_000_000),
            ("1999-01-01 00:00:00", 19_990_101_000_000),
        ];
        let mut ctx = EvalContext::default();
        for (expected, input) in cases {
            let actual = Time::parse_from_i64_default(&mut ctx, input)?;
            assert_eq!(actual.to_string(), expected);
            assert_eq!(actual.fsp(), DEFAULT_FSP as u8);
        }

        let should_fail = vec![-1111, 1, 100, 700_100, 100_000_000, 100_000_101_000_000];
        for case in should_fail {
            Time::parse_from_i64_default(&mut ctx, case).unwrap_err();
        }
        Ok(())
    }

    #[test]
    fn test_parse_from_real() -> Result<()> {
        let cases = vec![
            ("2000-03-05 00:00:00", "305", 0),
            ("2000-12-03 00:00:00", "1203", 0),
            ("2003-12-05 00:00:00.0", "31205", 1),
            ("2007-01-18 00:00:00.00", "070118", 2),
            ("0101-12-09 00:00:00.000", "1011209.333", 3),
            ("2017-01-18 00:00:00.0000", "20170118.123", 4),
            ("2012-12-31 11:30:45.12335", "121231113045.123345", 5),
            ("2012-12-31 11:30:45.125000", "20121231113045.123345", 6),
            ("2012-12-31 11:30:46.00000", "121231113045.9999999", 5),
            ("2017-01-05 08:40:59.5756", "170105084059.575601", 4),
        ];
        let mut ctx = EvalContext::default();
        for (expected, input, fsp) in cases {
            let input: Real = input.parse().unwrap();
            let actual_real =
                Time::parse_from_real(&mut ctx, &input, TimeType::DateTime, fsp, true)?;
            assert_eq!(actual_real.to_string(), expected);
        }

        let should_fail = vec![
            "201705051315111.22",
            "2011110859.1111",
            "2011110859.1111",
            "191203081.1111",
            "43128.121105",
        ];

        for case in should_fail {
            let case: Real = case.parse().unwrap();
            Time::parse_from_real(&mut ctx, &case, TimeType::DateTime, 0, true).unwrap_err();
        }
        Ok(())
    }

    #[test]
    fn test_parse_from_real_default() -> Result<()> {
        let cases = vec![
            ("2000-03-05", "305"),
            ("2000-12-03", "1203"),
            ("2003-12-05", "31205"),
            ("2007-01-18", "070118"),
            ("0101-12-09", "1011209.333"),
            ("2017-01-18", "20170118.123"),
            ("2012-12-31 11:30:45.123352", "121231113045.123345"),
            ("2012-12-31 11:30:45.125000", "20121231113045.123345"),
            ("2012-12-31 11:30:46.000000", "121231113045.9999999"),
            ("2017-01-05 08:40:59.575592", "170105084059.575601"),
        ];
        let mut ctx = EvalContext::default();
        for (expected, input) in cases {
            let input: Real = input.parse().unwrap();
            let actual_real = Time::parse_from_real_default(&mut ctx, &input)?;
            assert_eq!(actual_real.to_string(), expected);
        }

        let should_fail = vec![
            "201705051315111.22",
            "2011110859.1111",
            "2011110859.1111",
            "191203081.1111",
            "43128.121105",
        ];

        for case in should_fail {
            let case: Real = case.parse().unwrap();
            Time::parse_from_real_default(&mut ctx, &case).unwrap_err();
        }
        Ok(())
    }

    #[test]
    fn test_parse_from_decimal() -> Result<()> {
        let cases = vec![
            ("2000-03-05 00:00:00", "305", 0),
            ("2000-12-03 00:00:00", "1203", 0),
            ("2003-12-05 00:00:00.0", "31205", 1),
            ("2007-01-18 00:00:00.00", "070118", 2),
            ("0101-12-09 00:00:00.000", "1011209.333", 3),
            ("2017-01-18 00:00:00.0000", "20170118.123", 4),
            ("2012-12-31 11:30:45.12335", "121231113045.123345", 5),
            ("2012-12-31 11:30:45.123345", "20121231113045.123345", 6),
            ("2012-12-31 11:30:46.00000", "121231113045.9999999", 5),
            ("2017-01-05 08:40:59.5756", "170105084059.575601", 4),
        ];
        let mut ctx = EvalContext::default();
        for (expected, input, fsp) in cases {
            let input: Decimal = input.parse().unwrap();
            let actual = Time::parse_from_decimal(&mut ctx, &input, TimeType::DateTime, fsp, true)?;
            assert_eq!(actual.to_string(), expected);
        }

        let should_fail = vec![
            "201705051315111.22",
            "2011110859.1111",
            "2011110859.1111",
            "191203081.1111",
            "43128.121105",
        ];
        for case in should_fail {
            let case: Decimal = case.parse().unwrap();
            Time::parse_from_decimal(&mut ctx, &case, TimeType::DateTime, 0, true).unwrap_err();
        }
        Ok(())
    }

    #[test]
    fn test_parse_from_decimal_default() -> Result<()> {
        let cases = vec![
            ("2000-03-05", "305"),
            ("2000-12-03", "1203"),
            ("2003-12-05", "31205"),
            ("2007-01-18", "070118"),
            ("0101-12-09", "1011209.333"),
            ("2017-01-18", "20170118.123"),
            ("2012-12-31 11:30:45.123345", "121231113045.123345"),
            ("2012-12-31 11:30:45.123345", "20121231113045.123345"),
            ("2012-12-31 11:30:45.999999", "121231113045.9999999"),
            ("2017-01-05 08:40:59.575601", "170105084059.575601"),
        ];
        let mut ctx = EvalContext::default();
        for (expected, input) in cases {
            let input: Decimal = input.parse().unwrap();
            let actual = Time::parse_from_decimal_default(&mut ctx, &input)?;
            assert_eq!(actual.to_string(), expected);
        }

        let should_fail = vec![
            "201705051315111.22",
            "2011110859.1111",
            "2011110859.1111",
            "191203081.1111",
            "43128.121105",
        ];
        for case in should_fail {
            let case: Decimal = case.parse().unwrap();
            Time::parse_from_decimal_default(&mut ctx, &case).unwrap_err();
        }
        Ok(())
    }

    #[test]
    fn test_parse_valid_date() -> Result<()> {
        let mut ctx = EvalContext::default();
        let cases = vec![
            ("2019-09-16", "20190916101112"),
            ("2019-09-16", "190916101112"),
            ("2019-09-16", "19091610111"),
            ("2019-09-16", "1909161011"),
            ("2019-09-16", "190916101"),
            ("1909-12-10", "19091210"),
            ("2019-09-16", "1909161"),
            ("2019-09-16", "190916"),
            ("2019-09-01", "19091"),
            ("2019-09-16", "190916101112.111"),
            ("2019-09-16", "20190916101112.111"),
            ("2019-09-16", "20190916101112.666"),
            ("2019-09-16", "20190916101112.999"),
            ("2019-09-16", "19-09-16 10:11:12"),
            ("2019-12-31", "2019-12-31"),
            ("2019-09-16", "2019-09-16 10:11:12"),
            ("2019-09-16", "2019-09-16T10:11:12"),
            ("2019-09-16", "2019-09-16T10:11:12.66"),
            ("2019-09-16", "2019-09-16T10:11:12.99"),
            ("2019-12-31", "2019-12-31 23:59:59.99"),
            ("2019-12-31", "2019-12-31 23:59:59.9999999"),
            ("2019-12-31", "2019-12-31 23:59:59.9999999"),
            ("2019-12-31", "2019-12-31 23:59:59.999999"),
            ("2019-12-31", "2019*12&31T23(59)59.999999"),
            ("2019-12-31", "2019.12.31.23.59.59.999999"),
            ("2019-12-31", "2019.12.31-23.59.59.999999"),
            ("2019-12-31", "2019.12.31(23.59.59.999999"),
            ("2019-12-31", "2019.12.31     23.59.59.999999"),
            ("2019-12-31", "2019.12.31 \t    23.59.59.999999"),
            ("2019-12-31", "2019.12.31 \t  23.59-59.999999"),
            ("2013-05-28", "1305280512.000000000000"),
            ("0000-00-00", "00:00:00"),
        ];

        for (expected, actual) in cases {
            let date = Time::parse_date(&mut ctx, actual)?;
            assert_eq!(date.hour(), 0);
            assert_eq!(date.minute(), 0);
            assert_eq!(date.second(), 0);
            assert_eq!(date.micro(), 0);
            assert_eq!(date.fsp(), 0);
            assert_eq!(expected, date.to_string());
        }

        let should_fail = vec![
            ("11-12-13 T 12:34:56"),
            ("11:12:13 T12:34:56"),
            ("11:12:13 T12:34:56.12"),
            ("11:12:13T25:34:56.12"),
            ("2011-12-13t12:34:56.12"),
            ("11:12:13T23:61:56.12"),
            ("11:12:13T23:59:89.12"),
            ("11121311121.1"),
            ("1201012736"),
            ("1201012736.0"),
            ("111213111.1"),
            ("11121311.1"),
            ("1112131.1"),
            ("111213.1"),
            ("111213.1"),
            ("11121.1"),
            ("1112"),
        ];

        for case in should_fail {
            Time::parse_date(&mut ctx, case).unwrap_err();
        }
        Ok(())
    }

    #[test]
    fn test_parse_valid_datetime() -> Result<()> {
        let mut ctx = EvalContext::default();
        let cases = vec![
            ("2019-09-16 10:11:12", "20190916101112", 0, false),
            ("2019-09-16 10:11:12", "190916101112", 0, false),
            ("2019-09-16 10:11:01", "19091610111", 0, false),
            ("2019-09-16 10:11:00", "1909161011", 0, false),
            ("2019-09-16 10:01:00", "190916101", 0, false),
            ("1909-12-10 00:00:00", "19091210", 0, false),
            ("2020-02-29 10:00:00", "20200229100000", 0, false),
            ("2019-09-16 01:00:00", "1909161", 0, false),
            ("2019-09-16 00:00:00", "190916", 0, false),
            ("2019-09-01 00:00:00", "19091", 0, false),
            ("2019-09-16 10:11:12.111", "190916101112.111", 3, false),
            ("2019-09-16 10:11:12.111", "20190916101112.111", 3, false),
            ("2019-09-16 10:11:12.67", "20190916101112.666", 2, true),
            ("2019-09-16 10:11:13.0", "20190916101112.999", 1, true),
            ("2019-09-16 00:00:00", "2019-09-16", 0, false),
            ("2019-09-16 10:11:12", "2019-09-16 10:11:12", 0, false),
            ("2019-09-16 10:11:12", "2019-09-16T10:11:12", 0, false),
            ("2019-09-16 10:11:12.7", "2019-09-16T10:11:12.66", 1, true),
            ("2019-09-16 10:11:13.0", "2019-09-16T10:11:12.99", 1, true),
            ("2020-01-01 00:00:00.0", "2019-12-31 23:59:59.99", 1, true),
            ("2013-05-28 05:12:00", "1305280512.000000000000", 0, true),
            ("2013-05-28 05:12:00", "1305280512", 0, true),
            (
                "2020-01-01 00:00:00.0",
                "    2019-12-31 23:59:59.99   ",
                1,
                true,
            ),
            (
                "2020-01-01 00:00:00.000000",
                "2019-12-31 23:59:59.9999999",
                6,
                true,
            ),
            (
                "2020-01-01 00:00:00.000000",
                "2019-12(31-23.59.59.9999999",
                6,
                true,
            ),
            (
                "2020-01-01 00:00:00.000000",
                "2019-12(31-23.59.59.9999999",
                6,
                true,
            ),
            (
                "2020-01-01 00:00:00.000000",
                "2019-12(31    \t23.59.59.9999999",
                6,
                true,
            ),
            (
                "2019-12-31 23:59:59.999999",
                "2019-12-31 23:59:59.9999999",
                6,
                false,
            ),
            (
                "2019-12-31 23:59:59.999",
                "2019-12-31  23:59:59.999999",
                3,
                false,
            ),
            (
                "2019-12-31 23:59:59.999",
                "2019*12&31T23(59)59.999999",
                3,
                false,
            ),
            ("0000-00-00 00:00:00", "00:00:00", 0, false),
            ("2020-12-23 15:59:10", "2020--12+-23 15:^59:-10", 0, false),
            ("2020-12-23 15:59:23", "2020-12-23 15:59:23Z", 0, false),
            ("2020-12-23 07:59:23", "2020-12-23 15:59:23+0800", 0, false),
            ("2020-12-23 23:59:23", "2020-12-23 15:59:23-08", 0, false),
            ("2020-12-23 07:59:23", "2020-12-23 15:59:23+08:00", 0, false),
            ("2022-06-02 11:59:30", "2022-06-02 11:59:30.123Z", 0, false),
            (
                "2022-06-02 03:59:30",
                "2022-06-02 11:59:30.123+0800",
                0,
                false,
            ),
            (
                "2022-06-02 19:59:30",
                "2022-06-02 11:59:30.123-08",
                0,
                false,
            ),
            (
                "2022-06-02 03:29:30",
                "2022-06-02 11:59:30.123+08:30",
                0,
                false,
            ),
        ];
        for (expected, actual, fsp, round) in cases {
            assert_eq!(
                expected,
                Time::parse_datetime(&mut ctx, actual, fsp, round)?.to_string()
            );
        }

        let should_fail = vec![
            ("11-12-13 T 12:34:56", 0),
            ("11:12:13 T12:34:56", 0),
            ("11:12:13 T12:34:56.12", 7),
            ("11:12:13T25:34:56.12", 7),
            ("11:12:13T23:61:56.12", 7),
            ("11:12:13T23:59:89.12", 7),
            ("11121311121.1", 2),
            ("1201012736", 2),
            ("1201012736.0", 2),
            ("111213111.1", 2),
            ("11121311.1", 2),
            ("1112131.1", 2),
            ("111213.1", 2),
            ("111213.1", 2),
            ("11121.1", 2),
            ("1112", 2),
        ];

        for (case, fsp) in should_fail {
            Time::parse_datetime(&mut ctx, case, fsp, false).unwrap_err();
        }
        Ok(())
    }

    #[test]
    fn test_parse_valid_timestamp() -> Result<()> {
        let mut ctx = EvalContext::default();
        let cases = vec![
            ("2019-09-16 10:11:12", "20190916101112", 0, false),
            ("2019-09-16 10:11:12", "190916101112", 0, false),
            ("2019-09-16 10:11:01", "19091610111", 0, false),
            ("2019-09-16 10:11:00", "1909161011", 0, false),
            ("2019-09-16 10:01:00", "190916101", 0, false),
            ("2019-12-10 00:00:00", "20191210", 0, false),
            ("2019-09-16 01:00:00", "1909161", 0, false),
            ("2019-09-16 00:00:00", "190916", 0, false),
            ("2019-09-01 00:00:00", "19091", 0, false),
            ("2019-09-16 10:11:12.111", "190916101112.111", 3, false),
            ("2019-09-16 10:11:12.111", "20190916101112.111", 3, false),
            ("2019-09-16 10:11:12.67", "20190916101112.666", 2, true),
            ("2019-09-16 10:11:13.0", "20190916101112.999", 1, true),
            ("2019-09-16 00:00:00", "2019-09-16", 0, false),
            ("2019-09-16 10:11:12", "2019-09-16 10:11:12", 0, false),
            ("2019-09-16 10:11:12", "2019-09-16T10:11:12", 0, false),
            ("2019-09-16 10:11:12.7", "2019-09-16T10:11:12.66", 1, true),
            ("2019-09-16 10:11:13.0", "2019-09-16T10:11:12.99", 1, true),
            ("2020-01-01 00:00:00.0", "2019-12-31 23:59:59.99", 1, true),
            ("1970-01-01 00:00:00", "1970-01-01 00:00:00", 0, false),
            ("1970-01-01 00:00:00", "1970-1-1 00:00:00", 0, false),
            ("1970-01-01 12:13:09", "1970-1-1 12:13:9", 0, false),
            ("1970-01-01 09:08:09", "1970-1-1 9:8:9", 0, false),
            ("2013-05-28 05:12:00", "1305280512.000000000000", 0, true),
            ("2013-05-28 05:12:00", "1305280512", 0, true),
            (
                "2020-01-01 00:00:00.000000",
                "2019-12-31 23:59:59.9999999",
                6,
                true,
            ),
            (
                "2019-12-31 23:59:59.999999",
                "2019-12-31 23:59:59.9999999",
                6,
                false,
            ),
            (
                "2019-12-31 23:59:59.999",
                "2019-12-31     23:59:59.999999",
                3,
                false,
            ),
            (
                "2019-12-31 23:59:59.999",
                "2019*12&31T23(59)59.999999",
                3,
                false,
            ),
            ("0000-00-00 00:00:00", "00:00:00", 0, false),
        ];
        for (expected, actual, fsp, round) in cases {
            assert_eq!(
                expected,
                Time::parse_timestamp(&mut ctx, actual, fsp, round)?.to_string()
            );
        }
        Ok(())
    }

    #[test]
    fn test_parse_time_with_tz() -> Result<()> {
        let ctx_with_tz = |tz: &str, by_offset: bool| {
            let mut cfg = EvalConfig::default();
            if by_offset {
                let raw = tz.as_bytes();
                // brutally turn timezone in format +08:00 into offset in minute
                let offset = if raw[0] == b'-' { -1 } else { 1 }
                    * ((raw[1] - b'0') as i64 * 10 + (raw[2] - b'0') as i64)
                    * 60
                    + ((raw[4] - b'0') as i64 * 10 + (raw[5] - b'0') as i64);
                cfg.set_time_zone_by_offset(offset * 60).unwrap();
            } else {
                cfg.set_time_zone_by_name(tz).unwrap();
            }
            let warnings = cfg.new_eval_warnings();
            EvalContext {
                cfg: Arc::new(cfg),
                warnings,
            }
        };
        struct Case {
            tz: &'static str,
            by_offset: bool,
            t: &'static str,
            r: Option<&'static str>,
            tp: TimeType,
        }
        let cases = vec![
            Case {
                tz: "+00:00",
                by_offset: true,
                t: "2020-10-10T10:10:10Z",
                r: Some("2020-10-10 10:10:10.000000"),
                tp: TimeType::DateTime,
            },
            Case {
                tz: "+00:00",
                by_offset: true,
                t: "2020-10-10T10:10:10+",
                r: None,
                tp: TimeType::DateTime,
            },
            Case {
                tz: "+00:00",
                by_offset: true,
                t: "2020-10-10T10:10:10+14:01",
                r: None,
                tp: TimeType::DateTime,
            },
            Case {
                tz: "+00:00",
                by_offset: true,
                t: "2020-10-10T10:10:10-00:00",
                r: None,
                tp: TimeType::DateTime,
            },
            Case {
                tz: "-08:00",
                by_offset: true,
                t: "2020-10-10T10:10:10-08",
                r: Some("2020-10-10 10:10:10.000000"),
                tp: TimeType::DateTime,
            },
            Case {
                tz: "+08:00",
                by_offset: true,
                t: "2020-10-10T10:10:10+08:00",
                r: Some("2020-10-10 10:10:10.000000"),
                tp: TimeType::DateTime,
            },
            Case {
                tz: "+08:00",
                by_offset: true,
                t: "2020-10-10T10:10:10+08:00",
                r: Some("2020-10-10 10:10:10.000000"),
                tp: TimeType::Timestamp,
            },
            Case {
                tz: "+08:00",
                by_offset: true,
                t: "2022-06-02T10:10:10Z",
                r: Some("2022-06-02 18:10:10.000000"),
                tp: TimeType::DateTime,
            },
            Case {
                tz: "-08:00",
                by_offset: true,
                t: "2022-06-02T10:10:10Z",
                r: Some("2022-06-02 02:10:10.000000"),
                tp: TimeType::DateTime,
            },
            Case {
                tz: "+06:30",
                by_offset: true,
                t: "2022-06-02T10:10:10-05:00",
                r: Some("2022-06-02 21:40:10.000000"),
                tp: TimeType::DateTime,
            },
            // Time with fraction
            Case {
                tz: "+08:00",
                by_offset: true,
                t: "2022-06-02T10:10:10.123Z",
                r: Some("2022-06-02 18:10:10.123000"),
                tp: TimeType::DateTime,
            },
            Case {
                tz: "-08:00",
                by_offset: true,
                t: "2022-06-02T10:10:10.123Z",
                r: Some("2022-06-02 02:10:10.123000"),
                tp: TimeType::DateTime,
            },
            Case {
                tz: "+06:30",
                by_offset: true,
                t: "2022-06-02T10:10:10.654321-05:00",
                r: Some("2022-06-02 21:40:10.654321"),
                tp: TimeType::DateTime,
            },
            Case {
                // Note: this case may fail if Brazil observes DST again.
                // See https://github.com/pingcap/tidb/issues/49586
                tz: "Brazil/East",
                by_offset: false,
                t: "2023-11-30T17:02:00.654321+00:00",
                r: Some("2023-11-30 14:02:00.654321"),
                tp: TimeType::DateTime,
            },
        ];
        let mut result: Vec<Option<String>> = vec![];
        for Case {
            tz,
            by_offset,
            t,
            r: _,
            tp,
        } in &cases
        {
            let mut ctx = ctx_with_tz(tz, *by_offset);
            let parsed = Time::parse(&mut ctx, t, *tp, 6, true);
            match parsed {
                Ok(p) => result.push(Some(p.to_string())),
                Err(_) => result.push(None),
            }
        }
        for (a, b) in result.into_iter().zip(cases) {
            match (a, b.r) {
                (Some(a), Some(b)) => assert_eq!(a.as_str(), b),
                (None, None) => {}
                _ => {
                    return Err(Error::invalid_time_format(b.t));
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_allow_invalid_date() -> Result<()> {
        let cases = vec![
            ("2019-02-31", "2019-2-31"),
            ("2019-02-29", "2019-2-29"),
            ("2019-04-31", "2019-4-31"),
            ("0000-00-00", "2019-1-32"),
            ("0000-00-00", "2019-13-1"),
            ("2019-02-11", "2019-02-11"),
            ("2019-02-00", "2019-02-00"),
        ];

        for (expected, actual) in cases {
            let mut ctx = EvalContext::from(TimeEnv {
                allow_invalid_date: true,
                ..TimeEnv::default()
            });
            assert_eq!(expected, Time::parse_date(&mut ctx, actual)?.to_string());
        }
        Ok(())
    }

    #[test]
    fn test_invalid_datetime() -> Result<()> {
        let mut ctx = EvalContext::from(TimeEnv {
            allow_invalid_date: true,
            ..TimeEnv::default()
        });

        let cases = vec![
            "2019-12-31 24:23:22",
            "2019-12-31 23:60:22",
            "2019-12-31 23:24:60",
        ];

        for case in cases {
            assert_eq!(
                "0000-00-00 00:00:00",
                Time::parse_datetime(&mut ctx, case, 0, false)?.to_string()
            );
        }
        Ok(())
    }

    #[test]
    fn test_allow_invalid_timestamp() -> Result<()> {
        let mut ctx = EvalContext::from(TimeEnv {
            allow_invalid_date: true,
            ..TimeEnv::default()
        });

        let ok_cases = vec![
            "2019-9-31 11:11:11",
            "2019-0-1 11:11:11",
            "2013-2-29 11:11:11",
        ];
        for case in ok_cases {
            assert_eq!(
                "0000-00-00 00:00:00",
                Time::parse_timestamp(&mut ctx, case, 0, false)?.to_string()
            );
        }

        let dsts = vec![
            ("2019-03-10 02:00:00", "America/New_York"),
            ("2018-04-01 02:00:00", "America/Monterrey"),
        ];
        for (timestamp, time_zone) in dsts {
            let mut ctx = EvalContext::from(TimeEnv {
                allow_invalid_date: true,
                time_zone: Tz::from_tz_name(time_zone),
                ..TimeEnv::default()
            });
            assert_eq!(
                "0000-00-00 00:00:00",
                Time::parse_timestamp(&mut ctx, timestamp, 0, false)?.to_string()
            )
        }

        Ok(())
    }

    #[test]
    fn test_no_zero_date() -> Result<()> {
        // Enable NO_ZERO_DATE only. If zero-date is encountered, a warning is produced.
        let mut ctx = EvalContext::from(TimeEnv {
            no_zero_date: true,
            ..TimeEnv::default()
        });

        let _ = Time::parse_datetime(&mut ctx, "0000-00-00 00:00:00", 0, false)?;

        assert!(ctx.warnings.warning_cnt > 0);

        // Enable both NO_ZERO_DATE and STRICT_MODE.
        // If zero-date is encountered, an error is returned.
        let mut ctx = EvalContext::from(TimeEnv {
            no_zero_date: true,
            strict_mode: true,
            ..TimeEnv::default()
        });

        Time::parse_datetime(&mut ctx, "0000-00-00 00:00:00", 0, false).unwrap_err();

        // Enable NO_ZERO_DATE, STRICT_MODE and IGNORE_TRUNCATE.
        // If zero-date is encountered, an error is returned.
        let mut ctx = EvalContext::from(TimeEnv {
            no_zero_date: true,
            strict_mode: true,
            ignore_truncate: true,
            ..TimeEnv::default()
        });

        assert_eq!(
            "0000-00-00 00:00:00",
            Time::parse_datetime(&mut ctx, "0000-00-00 00:00:00", 0, false)?.to_string()
        );

        assert!(ctx.warnings.warning_cnt > 0);

        let cases = vec![
            "2019-12-31 24:23:22",
            "2019-12-31 23:60:22",
            "2019-12-31 23:24:60",
        ];

        for case in cases {
            // Enable NO_ZERO_DATE, STRICT_MODE and ALLOW_INVALID_DATE.
            // If an invalid date (converted to zero-date) is encountered, an error is
            // returned.
            let mut ctx = EvalContext::from(TimeEnv {
                no_zero_date: true,
                strict_mode: true,
                ..TimeEnv::default()
            });
            Time::parse_datetime(&mut ctx, case, 0, false).unwrap_err();
        }

        Ok(())
    }

    #[test]
    fn test_no_zero_in_date() -> Result<()> {
        let cases = ["2019-01-00", "2019-00-01"];

        for &case in cases.iter() {
            // Enable NO_ZERO_IN_DATE only. If zero-date is encountered, a warning is
            // produced.
            let mut ctx = EvalContext::from(TimeEnv {
                no_zero_in_date: true,
                ..TimeEnv::default()
            });

            let _ = Time::parse_datetime(&mut ctx, case, 0, false)?;

            assert!(ctx.warnings.warning_cnt > 0);
        }

        // Enable NO_ZERO_IN_DATE, STRICT_MODE and IGNORE_TRUNCATE.
        // If zero-date is encountered, an error is returned.
        let mut ctx = EvalContext::from(TimeEnv {
            no_zero_in_date: true,
            strict_mode: true,
            ignore_truncate: true,
            ..TimeEnv::default()
        });

        assert_eq!(
            "0000-00-00 00:00:00",
            Time::parse_datetime(&mut ctx, "0000-00-00 00:00:00", 0, false)?.to_string()
        );

        assert!(ctx.warnings.warning_cnt > 0);

        for &case in cases.iter() {
            // Enable both NO_ZERO_IN_DATE and STRICT_MODE,.
            // If zero-date is encountered, an error is returned.
            let mut ctx = EvalContext::from(TimeEnv {
                no_zero_in_date: true,
                strict_mode: true,
                ..TimeEnv::default()
            });
            Time::parse_datetime(&mut ctx, case, 0, false).unwrap_err();
        }

        Ok(())
    }

    #[test]
    fn test_codec_datetime() -> Result<()> {
        let cases = vec![
            ("2010-10-10 10:11:11", 0),
            ("2017-01-01 00:00:00", 0),
            ("2004-01-01 00:00:00", UNSPECIFIED_FSP),
            ("2013-01-01 00:00:00.000000", MAX_FSP),
            ("2019-01-01 00:00:00.123456", MAX_FSP),
            ("2001-01-01 00:00:00.123456", MAX_FSP),
            ("2007-06-01 00:00:00.999999", MAX_FSP),
            // Invalid cases
            ("0000-00-00 00:00:00", 0),
            ("2007-00-01 00:00:00.999999", MAX_FSP),
            ("2017-01-00 00:00:00.999999", MAX_FSP),
            ("2027-00-00 00:00:00.999999", MAX_FSP),
            ("2027-04-31 00:00:00.999999", MAX_FSP),
        ];

        for (case, fsp) in cases {
            let mut ctx = EvalContext::from(TimeEnv {
                allow_invalid_date: true,
                ..TimeEnv::default()
            });
            let time = Time::parse_datetime(&mut ctx, case, fsp, false)?;

            let packed = time.to_packed_u64(&mut ctx)?;
            let reverted_datetime =
                Time::from_packed_u64(&mut ctx, packed, TimeType::DateTime, fsp)?;

            assert_eq!(time, reverted_datetime);
        }

        Ok(())
    }

    #[test]
    fn test_codec_timestamp() -> Result<()> {
        let tz_table = vec!["Etc/GMT+11", "Etc/GMT0", "Etc/GMT-5", "UTC", "Universal"];

        let cases = vec![
            ("0000-00-00 00:00:00", 0),
            ("2010-10-10 10:11:11", 0),
            ("2017-01-01 00:00:00", 0),
            ("2004-01-01 00:00:00", UNSPECIFIED_FSP),
            ("2019-07-01 12:13:14.999", MAX_FSP),
            ("2013-01-01 00:00:00.000000", MAX_FSP),
            ("2019-04-01 00:00:00.123456", MAX_FSP),
            ("2001-01-01 00:00:00.123456", MAX_FSP),
            ("2007-08-01 00:00:00.999999", MAX_FSP),
        ];

        for tz in tz_table {
            for &(case, fsp) in cases.iter() {
                let mut ctx = EvalContext::from(TimeEnv {
                    time_zone: Tz::from_tz_name(tz),
                    ..TimeEnv::default()
                });

                let time = Time::parse_timestamp(&mut ctx, case, fsp, false)?;

                let packed = time.to_packed_u64(&mut ctx)?;
                let reverted_datetime =
                    Time::from_packed_u64(&mut ctx, packed, TimeType::Timestamp, fsp)?;

                assert_eq!(time, reverted_datetime);
            }
        }

        Ok(())
    }

    #[test]
    fn test_compare() -> Result<()> {
        let cases = vec![
            (
                "2019-03-17 12:13:14.11",
                "2019-03-17 12:13:14.11",
                Ordering::Equal,
            ),
            ("2019-4-1 1:2:3", "2019-3-31 23:59:59", Ordering::Greater),
            ("2019-09-16 1:2:3", "2019-10-01 1:2:1", Ordering::Less),
            ("0000-00-00", "0000-00-00", Ordering::Equal),
        ];

        for (left, right, expected) in cases {
            let mut ctx = EvalContext::default();
            let left = Time::parse_datetime(&mut ctx, left, MAX_FSP, false)?;
            let right = Time::parse_datetime(&mut ctx, right, MAX_FSP, false)?;
            assert_eq!(expected, left.cmp(&right));
        }
        Ok(())
    }

    #[test]
    fn test_from_duration() -> Result<()> {
        let cases = vec![
            ("11:30:45.123456", "2020-02-02 11:30:45.123456"),
            ("-35:30:46", "2020-01-31 12:29:14.000000"),
            ("25:59:59.999999", "2020-02-03 01:59:59.999999"),
        ];
        let mut cfg = EvalConfig::default();
        cfg.is_test = true;
        let mut ctx = EvalContext::new(Arc::new(cfg));
        for (case, expected) in cases {
            let duration = Duration::parse(&mut ctx, case, MAX_FSP)?;

            let actual = Time::from_duration(&mut ctx, duration, TimeType::DateTime)?;
            assert_eq!(actual.to_string(), expected);
        }
        Ok(())
    }

    #[test]
    fn test_from_local_time() -> Result<()> {
        let mut ctx = EvalContext::default();
        for i in 2..10 {
            let actual = Time::from_local_time(&mut ctx, TimeType::DateTime, i % MAX_FSP)?;
            let c_datetime = actual.try_into_chrono_datetime(&mut ctx)?;

            let now0 = c_datetime.timestamp_millis() as u64;
            let now1 = Utc::now().timestamp_millis() as u64;
            assert!(now1 - now0 < 1000, "{:?} {:?}", now1, now0);
        }
        Ok(())
    }

    #[test]
    fn test_round_frac() -> Result<()> {
        let cases = vec![
            ("121231113045.123345", 6, "2012-12-31 11:30:45.123345"),
            ("121231113045.999999", 6, "2012-12-31 11:30:45.999999"),
            ("121231113045.999999", 5, "2012-12-31 11:30:46.00000"),
            ("2012-12-31 11:30:45.123456", 4, "2012-12-31 11:30:45.1235"),
            (
                "2012-12-31 11:30:45.123456",
                6,
                "2012-12-31 11:30:45.123456",
            ),
            ("2012-12-31 11:30:45.123456", 0, "2012-12-31 11:30:45"),
            ("2012-12-31 11:30:45.9", 0, "2012-12-31 11:30:46"),
            ("2012-12-31 11:30:45.123456", 1, "2012-12-31 11:30:45.1"),
            ("2012-12-31 11:30:45.999999", 4, "2012-12-31 11:30:46.0000"),
            ("2012-12-31 11:30:45.999999", 0, "2012-12-31 11:30:46"),
            ("2012-12-31 23:59:59.999999", 0, "2013-01-01 00:00:00"),
            ("2012-12-31 23:59:59.999999", 3, "2013-01-01 00:00:00.000"),
            ("2012-00-00 11:30:45.999999", 3, "2012-00-00 11:30:46.000"),
            // Edge cases:
            ("2012-01-00 23:59:59.999999", 3, "0000-00-00 00:00:00.000"),
            ("2012-04-31 23:59:59.999999", 3, "0000-00-00 00:00:00.000"),
        ];

        for (input, fsp, expected) in cases {
            let mut ctx = EvalContext::from(TimeEnv {
                allow_invalid_date: true,
                ..TimeEnv::default()
            });
            assert_eq!(
                expected,
                Time::parse_datetime(&mut ctx, input, MAX_FSP, true)?
                    .round_frac(&mut ctx, fsp)?
                    .to_string()
            );
        }
        Ok(())
    }

    #[test]
    fn test_normalized() -> Result<()> {
        let should_pass = vec![
            ("2019-00-01 12:34:56.1", "2018-12-01 12:34:56.1"),
            ("2019-01-00 12:34:56.1", "2018-12-31 12:34:56.1"),
            ("2019-00-00 12:34:56.1", "2018-11-30 12:34:56.1"),
            ("2019-04-31 12:34:56.1", "2019-05-01 12:34:56.1"),
            ("2019-02-29 12:34:56.1", "2019-03-01 12:34:56.1"),
            ("2019-02-30 12:34:56.1", "2019-03-02 12:34:56.1"),
            ("2019-02-31 12:34:56.1", "2019-03-03 12:34:56.1"),
        ];
        for (input, expected) in should_pass {
            let mut ctx = EvalContext::from(TimeEnv {
                allow_invalid_date: true,
                ..TimeEnv::default()
            });
            assert_eq!(
                expected,
                Time::parse_datetime(&mut ctx, input, 1, false)?
                    .normalized(&mut ctx)?
                    .to_string()
            );
        }
        Ok(())
    }

    #[test]
    fn checked_add_sub_duration() -> Result<()> {
        let normal_cases = vec![
            (
                "2018-12-30 11:30:45.123456",
                "00:00:14.876545",
                "2018-12-30 11:31:00.000001",
            ),
            (
                "2018-12-30 11:30:45.123456",
                "00:30:00",
                "2018-12-30 12:00:45.123456",
            ),
            (
                "2018-12-30 11:30:45.123456",
                "12:30:00",
                "2018-12-31 00:00:45.123456",
            ),
            (
                "2018-12-30 11:30:45.123456",
                "1 12:30:00",
                "2019-01-01 00:00:45.123456",
            ),
        ];

        for (lhs, rhs, expected) in normal_cases.clone() {
            let mut ctx = EvalContext::default();
            let lhs = Time::parse_datetime(&mut ctx, lhs, 6, false)?;
            let rhs = Duration::parse(&mut ctx, rhs, 6)?;
            let actual = lhs.checked_add(&mut ctx, rhs).unwrap();
            assert_eq!(expected, actual.to_string());
        }

        for (expected, rhs, lhs) in normal_cases {
            let mut ctx = EvalContext::default();
            let lhs = Time::parse_datetime(&mut ctx, lhs, 6, false)?;
            let rhs = Duration::parse(&mut ctx, rhs, 6)?;
            let actual = lhs.checked_sub(&mut ctx, rhs).unwrap();
            assert_eq!(expected, actual.to_string());
        }

        // DSTs
        let mut ctx = EvalContext::from(TimeEnv {
            time_zone: Tz::from_tz_name("America/New_York"),
            ..TimeEnv::default()
        });
        let dsts = vec![
            ("2019-03-10 01:00:00", "1:00:00", "2019-03-10 03:00:00"),
            ("2018-03-11 01:00:00", "1:00:00", "2018-03-11 03:00:00"),
        ];

        for (lhs, rhs, expected) in dsts.clone() {
            let lhs = Time::parse_timestamp(&mut ctx, lhs, 0, false)?;
            let rhs = Duration::parse(&mut EvalContext::default(), rhs, 6)?;
            let actual = lhs.checked_add(&mut ctx, rhs).unwrap();
            assert_eq!(expected, actual.to_string());
        }

        for (expected, rhs, lhs) in dsts {
            let lhs = Time::parse_timestamp(&mut ctx, lhs, 0, false)?;
            let rhs = Duration::parse(&mut EvalContext::default(), rhs, 6)?;
            let actual = lhs.checked_sub(&mut ctx, rhs).unwrap();
            assert_eq!(expected, actual.to_string());
        }

        // Edge cases
        let mut ctx = EvalContext::from(TimeEnv {
            allow_invalid_date: true,
            ..TimeEnv::default()
        });
        let cases = vec![
            ("2019-04-31 00:00:00", "1:00:00", "2019-05-01 01:00:00"),
            ("2019-00-01 00:00:00", "1:00:00", "2018-12-01 01:00:00"),
            ("2019-2-0 00:00:00", "1:00:00", "2019-01-31 01:00:00"),
        ];
        for (lhs, rhs, expected) in cases {
            let lhs = Time::parse_datetime(&mut ctx, lhs, 0, false)?;
            let rhs = Duration::parse(&mut EvalContext::default(), rhs, 6)?;
            let actual = lhs.checked_add(&mut ctx, rhs).unwrap();
            assert_eq!(expected, actual.to_string());
        }

        // Failed cases
        let mut ctx = EvalContext::default();
        let lhs = Time::parse_datetime(&mut ctx, "9999-12-31 23:59:59", 6, false)?;
        let rhs = Duration::parse(&mut ctx, "01:00:00", 6)?;
        assert_eq!(lhs.checked_add(&mut ctx, rhs), None);

        let lhs = Time::parse_datetime(&mut ctx, "0000-01-01 00:00:01", 6, false)?;
        let rhs = Duration::parse(&mut ctx, "01:00:00", 6)?;
        assert_eq!(lhs.checked_sub(&mut ctx, rhs), None);

        Ok(())
    }

    #[test]
    fn test_weekday() -> Result<()> {
        let cases = vec![
            ("2019-10-12", "Sat"),
            ("2019-04-31", "Wed"),
            ("0000-01-01", "Sat"),
            ("0000-01-00", "Fri"),
        ];
        let mut ctx = EvalContext::from(TimeEnv {
            allow_invalid_date: true,
            ..TimeEnv::default()
        });
        for (s, expected) in cases {
            assert_eq!(
                expected,
                format!("{:?}", Time::parse_date(&mut ctx, s)?.weekday())
            );
        }
        Ok(())
    }

    #[test]
    fn test_date_format() -> Result<()> {
        let cases = vec![
            (
                "2010-01-07 23:12:34.12345",
                "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V
                %v %a %W %w %X %x %Y %y %%",
                "Jan January 01 1 7th 07 7 007 23 11 12 PM 11:12:34 PM 23:12:34 34 123450 01 01 01
                01 Thu Thursday 4 2010 2010 2010 10 %",
            ),
            (
                "2012-12-21 23:12:34.123456",
                "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U
                %u %V %v %a %W %w %X %x %Y %y %%",
                "Dec December 12 12 21st 21 21 356 23 11 12 PM 11:12:34 PM 23:12:34 34 123456 51
                51 51 51 Fri Friday 5 2012 2012 2012 12 %",
            ),
            (
                "0000-01-01 00:00:00.123456",
                // Functions week() and yearweek() don't support multi mode,
                // so the result of "%U %u %V %Y" is different from MySQL.
                "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %Y
                %y %%",
                "Jan January 01 1 1st 01 1 001 0 12 00 AM 12:00:00 AM 00:00:00 00 123456 52 0000
                00 %",
            ),
            (
                "2016-09-3 00:59:59.123456",
                "abc%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U
                %u %V %v %a %W %w %X %x %Y %y!123 %%xyz %z",
                "abcSep September 09 9 3rd 03 3 247 0 12 59 AM 12:59:59 AM 00:59:59 59 123456 35
                35 35 35 Sat Saturday 6 2016 2016 2016 16!123 %xyz z",
            ),
            (
                "2012-10-01 00:00:00",
                "%b %M %m %c %D %d %e %j %k %H %i %p %r %T %s %f %v
                %x %Y %y %%",
                "Oct October 10 10 1st 01 1 275 0 00 00 AM 12:00:00 AM 00:00:00 00 000000 40
                2012 2012 12 %",
            ),
        ];
        for (s, layout, expect) in cases {
            let mut ctx = EvalContext::default();
            let t = Time::parse_datetime(&mut ctx, s, 6, false)?;
            let get = t.date_format(layout)?;
            assert_eq!(get, expect);
        }
        Ok(())
    }

    #[test]
    fn test_to_numeric_string() {
        let cases = vec![
            ("2012-12-31 11:30:45.123456", 4, "20121231113045.1235"),
            ("2012-12-31 11:30:45.123456", 6, "20121231113045.123456"),
            ("2012-12-31 11:30:45.123456", 0, "20121231113045"),
            ("2012-12-31 11:30:45.999999", 0, "20121231113046"),
            ("2017-01-05 08:40:59.575601", 0, "20170105084100"),
            ("2017-01-05 23:59:59.575601", 0, "20170106000000"),
            ("0000-00-00 00:00:00", 6, "00000000000000.000000"),
        ];
        let mut ctx = EvalContext::default();
        for (s, fsp, expect) in cases {
            let t = Time::parse_datetime(&mut ctx, s, fsp, true).unwrap();
            let get = t.to_numeric_string();
            assert_eq!(get, expect);
        }
    }

    #[test]
    fn test_to_decimal() {
        let cases = vec![
            ("2012-12-31 11:30:45.123456", 4, "20121231113045.1235"),
            ("2012-12-31 11:30:45.123456", 6, "20121231113045.123456"),
            ("2012-12-31 11:30:45.123456", 0, "20121231113045"),
            ("2012-12-31 11:30:45.999999", 0, "20121231113046"),
            ("2017-01-05 08:40:59.575601", 0, "20170105084100"),
            ("2017-01-05 23:59:59.575601", 0, "20170106000000"),
            ("0000-00-00 00:00:00", 6, "0"),
        ];
        let mut ctx = EvalContext::default();
        for (s, fsp, expect) in cases {
            let t = Time::parse_datetime(&mut ctx, s, fsp, true).unwrap();
            let get: Decimal = t.convert(&mut ctx).unwrap();
            assert_eq!(
                get,
                expect.as_bytes().convert(&mut ctx).unwrap(),
                "convert datetime {} to decimal",
                s
            );
        }
    }

    #[test]
    #[allow(clippy::excessive_precision)]
    fn test_convert_to_f64() {
        let cases = vec![
            ("2012-12-31 11:30:45.123456", 4, 20121231113045.1235f64),
            ("2012-12-31 11:30:45.123456", 6, 20121231113045.123456f64),
            ("2012-12-31 11:30:45.123456", 0, 20121231113045f64),
            ("2012-12-31 11:30:45.999999", 0, 20121231113046f64),
            ("2017-01-05 08:40:59.575601", 0, 20170105084100f64),
            ("2017-01-05 23:59:59.575601", 0, 20170106000000f64),
            ("0000-00-00 00:00:00", 6, 0f64),
        ];
        let mut ctx = EvalContext::default();
        for (s, fsp, expect) in cases {
            let t = Time::parse_datetime(&mut ctx, s, fsp, true).unwrap();
            let get: f64 = t.convert(&mut ctx).unwrap();
            assert!(
                (expect - get).abs() < f64::EPSILON,
                "expect: {}, got: {}",
                expect,
                get
            );
        }
    }

    #[test]
    fn test_add_months() {
        // (input_year, input_month, input_day, add_months, expected_year,
        // expected_month, expected_day, should_err)
        let cases = vec![
            // Basic
            (2023, 1, 31, 1, 2023, 2, 28, false),
            (2024, 1, 31, 1, 2024, 2, 29, false),
            (2023, 5, 15, 2, 2023, 7, 15, false),
            (2023, 7, 30, 1, 2023, 8, 30, false),
            (2023, 8, 31, 1, 2023, 9, 30, false),
            // Leap year
            (2024, 1, 31, 1, 2024, 2, 29, false),
            (2024, 2, 29, 12, 2025, 2, 28, false),
            // Crossing over a year boundary
            (2023, 12, 15, 1, 2024, 1, 15, false),
            (2022, 12, 31, 2, 2023, 2, 28, false),
            // Decreasing months
            (2023, 3, 15, -3, 2022, 12, 15, false),
            (2023, 1, 31, -1, 2022, 12, 31, false),
            // Crossing a century (non-leap year to leap year)
            (1999, 12, 31, 1, 2000, 1, 31, false),
            (2000, 2, 29, 12, 2001, 2, 28, false),
            // Speical case
            (1, 1, 1, -1, 0, 0, 0, false),
            (1, 2, 1, -2, 0, 0, 0, false),
            (0, 1, 1, 0, 0, 0, 0, false),
            // Overflow
            (0, 1, 1, -1, 0, 0, 0, true),
            (9999, 12, 31, 1, 0, 0, 0, true),
        ];

        // Iterate over each test case and run the test
        for case in cases {
            let (
                input_year,
                input_month,
                input_day,
                add_months,
                expected_year,
                expected_month,
                expected_day,
                should_err,
            ) = case;

            let mut time = Time::default();
            time.set_year(input_year);
            time.set_month(input_month);
            time.set_day(input_day);

            let result = time.add_months(add_months);
            if should_err {
                assert!(
                    result.is_err(),
                    "Test case with input: {:?} should have error",
                    case
                );
            } else {
                assert!(
                    result.is_ok(),
                    "Test case with input: {:?} should not have error",
                    case
                );
                assert_eq!(
                    time.get_year(),
                    expected_year,
                    "Year mismatch for input: {:?}",
                    case
                );
                assert_eq!(
                    time.get_month(),
                    expected_month,
                    "Month mismatch for input: {:?}",
                    case
                );
                assert_eq!(
                    time.get_day(),
                    expected_day,
                    "Day mismatch for input: {:?}",
                    case
                );
            }
        }
    }

    #[test]
    fn test_add_sec_nanos() -> Result<()> {
        // (initial time, sec, nano, expected time, should_overflow)
        let cases = vec![
            (
                "2023-01-01 00:00:00",
                2,
                123000 * NANOS_PER_MICRO,
                "2023-01-01 00:00:02.123000",
                false,
            ),
            (
                "2023-01-01 00:00:00",
                0,
                -NANOS_PER_SEC,
                "2022-12-31 23:59:59.000000",
                false,
            ),
            (
                "2023-12-31 23:59:59",
                1,
                0,
                "2024-01-01 00:00:00.000000",
                false,
            ),
            (
                "2023-01-01 00:00:00",
                3 * SECS_PER_DAY,
                2 * NANOS_PER_DAY,
                "2023-01-06 00:00:00.000000",
                false,
            ),
            (
                "2024-02-27 00:00:00",
                2 * SECS_PER_DAY + 6 * SECS_PER_HOUR + 3 * SECS_PER_MINUTE + 55,
                NANOS_PER_DAY + 6 * NANOS_PER_HOUR + 123456 * NANOS_PER_MICRO,
                "2024-03-01 12:03:55.123456",
                false,
            ),
            (
                "0001-01-01 00:00:00",
                -2 * SECS_PER_DAY,
                0,
                "0000-00-00 00:00:00.000000",
                false,
            ),
            (
                "2023-01-01 00:00:00",
                10000 * SECS_PER_DAY,
                0,
                "2050-05-19 00:00:00.000000",
                false,
            ),
            (
                "0001-01-01 00:00:00",
                -SECS_PER_DAY,
                0,
                "0000-00-00 00:00:00.000000",
                false,
            ),
            ("0000-01-01 00:00:00", -2 * SECS_PER_DAY, 0, "", true),
            (
                "2024-01-01 00:00:00",
                10000 * 365 * SECS_PER_DAY + 1,
                0,
                "",
                true,
            ),
            (
                "2024-01-01 00:00:00",
                -10000 * 365 * SECS_PER_DAY - 1,
                0,
                "",
                true,
            ),
        ];

        let mut ctx = EvalContext::default();
        for case in cases {
            let (input, sec, nano, expected, should_overflow) = case;
            let time = Time::parse_datetime(&mut ctx, input, 6, false)?;
            let result = time.add_sec_nanos(&mut ctx, sec, nano);
            if should_overflow {
                assert!(
                    result.is_err(),
                    "Test case with input: {:?} should have error, result {}",
                    case,
                    result.unwrap()
                );
            } else {
                assert!(
                    result.is_ok(),
                    "Test case with input: {:?} should not have error, result {:?}",
                    case,
                    result
                );
                assert_eq!(
                    expected,
                    result.unwrap().to_string(),
                    "result mismatch for input: {:?}",
                    case
                );
            }
        }

        Ok(())
    }

    #[test]
    fn test_get_daynr() {
        let test_cases = [
            (0, 0, 0, 0),
            (0, 1, 1, 1),
            (1, 1, 1, 366),
            (2023, 10, 7, 739165),
            (9999, 12, 31, 3652424),
            (1970, 1, 1, 719528),
            (2006, 12, 16, 733026),
            (10, 1, 2, 3654),
            (2008, 2, 20, 733457),
        ];
        for (year, month, day, expected) in test_cases {
            let result = Time::get_daynr(year, month, day);
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_timestamp_diff() {
        let test_cases = vec![
            (
                "2025-02-05 12:00:00",
                "2025-02-05 12:00:00",
                IntervalUnit::Microsecond,
                0,
            ),
            (
                "2025-02-05 12:00:00",
                "2025-02-05 12:00:00.000001",
                IntervalUnit::Microsecond,
                1,
            ),
            (
                "2025-02-05 12:00:00",
                "2025-02-05 12:00:10",
                IntervalUnit::Second,
                10,
            ),
            (
                "2025-02-05 12:00:00",
                "2025-02-05 12:10:00",
                IntervalUnit::Minute,
                10,
            ),
            (
                "2025-02-05 12:00:00",
                "2025-02-05 14:00:00",
                IntervalUnit::Hour,
                2,
            ),
            (
                "2025-02-05 12:00:00",
                "2025-02-07 12:00:00",
                IntervalUnit::Day,
                2,
            ),
            (
                "2025-02-05 12:00:00",
                "2025-03-05 12:00:00",
                IntervalUnit::Month,
                1,
            ),
            (
                "2025-02-05 12:00:00",
                "2025-08-05 12:00:00",
                IntervalUnit::Quarter,
                2,
            ),
            (
                "2025-02-05 12:00:00",
                "2027-02-05 12:00:00",
                IntervalUnit::Year,
                2,
            ),
            (
                "2024-02-29 12:00:00",
                "2025-02-28 12:00:00",
                IntervalUnit::Year,
                0,
            ),
            (
                "2024-12-31 23:59:59",
                "2025-01-01 00:00:00",
                IntervalUnit::Second,
                1,
            ),
            (
                "2024-03-31 23:59:59",
                "2024-04-01 00:00:00",
                IntervalUnit::Second,
                1,
            ),
            (
                "2024-02-28 12:00:00",
                "2024-02-29 12:00:00",
                IntervalUnit::Day,
                1,
            ),
            (
                "2024-02-28 12:00:00",
                "2024-03-01 12:00:00",
                IntervalUnit::Day,
                2,
            ),
            (
                "2025-02-05 12:00:00",
                "2028-02-05 12:00:00",
                IntervalUnit::Year,
                3,
            ),
            (
                "2025-02-05 12:00:00",
                "2025-12-05 12:00:00",
                IntervalUnit::Month,
                10,
            ),
            (
                "2025-02-05 12:00:00",
                "2025-02-19 12:00:00",
                IntervalUnit::Week,
                2,
            ),
            (
                "2025-02-05 12:00:00",
                "2025-02-05 16:00:00",
                IntervalUnit::Hour,
                4,
            ),
            (
                "2025-02-05 12:00:00",
                "2025-02-06 00:00:00",
                IntervalUnit::Hour,
                12,
            ),
        ];

        let mut ctx = EvalContext::default();
        for (start, end, unit, expected) in test_cases {
            let start_dt = Time::parse_timestamp(&mut ctx, start, MAX_FSP, false).unwrap();
            let end_dt = Time::parse_timestamp(&mut ctx, end, MAX_FSP, false).unwrap();
            let result = start_dt.timestamp_diff(&end_dt, unit).unwrap();
            assert_eq!(
                result, expected,
                "Failed for {} -> {} in {:?}",
                start, end, unit
            );
        }
    }
}
