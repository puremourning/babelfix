//! Generated FIX schema constants for babelfix.
//!
//! The contents of this crate are generated at build time by parsing the FIX
//! Orchestra data embedded in the `babelfix-repo` crate. For each FIX version it
//! emits a module of tag-number constants, one `Fields` submodule per version:
//!
//! ```text
//! babelfix_repogen::FIX_4_2::Fields::*
//! babelfix_repogen::FIX_4_4::Fields::*
//! babelfix_repogen::FIX_Latest::Fields::*
//! ```
//!
//! Each entry is a `pub const NAME: u32` giving the field's tag number — for
//! example `FIX_Latest::Fields::MsgSeqNum` is `34`. Because tag numbers are
//! shared across versions, `FIX_Latest::Fields` is normally used regardless of
//! the runtime version selected from the repository.
//!
//! This crate is re-exported from the `babelfix` crate as `babelfix::schema`.

#![allow(non_upper_case_globals, non_snake_case)]
include!(concat!(env!("OUT_DIR"), "/repo_static.rs"));
