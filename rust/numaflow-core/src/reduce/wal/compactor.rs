//! Compactor has business logic. It knows what kind of WALs have been created and will
//! compact based on the type. WAL inherently is agnostic to data. The compactor will be given
//! multiple WAL types (data, gc, etc.) and it decides how to purge (aka compact).

pub(crate) enum Kind {
    Aligned,
    Unaligned,
}

pub(crate) struct Compactor {
    kind: Kind,
}