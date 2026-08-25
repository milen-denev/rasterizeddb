use chrono::{DateTime, FixedOffset};
use sea_orm::entity::prelude::*;
use uuid::Uuid;

/// Rust-side enum stored as a small integer — the portable alternative to a
/// native Postgres enum type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumIter, DeriveActiveEnum)]
#[sea_orm(rs_type = "i16", db_type = "SmallInteger")]
pub enum SubscriptionKind {
    #[sea_orm(num_value = 0)]
    Single,
    #[sea_orm(num_value = 1)]
    Organization,
    #[sea_orm(num_value = 2)]
    Free,
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "subscriptions")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,
    pub name: String,
    pub date_introduced: DateTime<FixedOffset>,
    pub last_charge: Option<DateTime<FixedOffset>>,
    pub user_id: Uuid,
    #[sea_orm(column_name = "group_name")]
    pub group: String,
    pub kind: SubscriptionKind,
    pub allow_new_domains: bool,
    pub infinite: bool,
    pub status: String,
    pub active_until: Option<DateTime<FixedOffset>>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::user::Entity",
        from = "Column::UserId",
        to = "super::user::Column::Id"
    )]
    User,
    #[sea_orm(has_many = "super::invoice::Entity")]
    Invoice,
}

impl Related<super::user::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::User.def()
    }
}

impl Related<super::invoice::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Invoice.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
