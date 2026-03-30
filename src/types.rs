/// Git object types matching the smallint values stored in PostgreSQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum ObjectType {
    Commit = 1,
    Tree = 2,
    Blob = 3,
    Tag = 4,
}

impl ObjectType {
    pub fn from_i16(val: i16) -> Option<Self> {
        match val {
            1 => Some(Self::Commit),
            2 => Some(Self::Tree),
            3 => Some(Self::Blob),
            4 => Some(Self::Tag),
            _ => None,
        }
    }

    pub fn to_git2(self) -> git2::ObjectType {
        match self {
            Self::Commit => git2::ObjectType::Commit,
            Self::Tree => git2::ObjectType::Tree,
            Self::Blob => git2::ObjectType::Blob,
            Self::Tag => git2::ObjectType::Tag,
        }
    }

    pub fn from_git2(t: git2::ObjectType) -> Option<Self> {
        match t {
            git2::ObjectType::Commit => Some(Self::Commit),
            git2::ObjectType::Tree => Some(Self::Tree),
            git2::ObjectType::Blob => Some(Self::Blob),
            git2::ObjectType::Tag => Some(Self::Tag),
            _ => None,
        }
    }
}
