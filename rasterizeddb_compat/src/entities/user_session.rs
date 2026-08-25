use chrono::{DateTime, FixedOffset};
use sea_orm::entity::prelude::*;
use uuid::Uuid;

/// Heavy on nullable columns and timestamp comparisons — the shape most session
/// lookups take (`expires_at > now AND revoked_at IS NULL`).
#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "user_session")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,
    pub user_id: Uuid,
    pub domain_id: Uuid,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
    pub approx_location: Option<String>,
    pub created_at: DateTime<FixedOffset>,
    pub last_seen_at: DateTime<FixedOffset>,
    pub expires_at: DateTime<FixedOffset>,
    pub revoked_at: Option<DateTime<FixedOffset>>,
    pub reverify_required: bool,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::user::Entity",
        from = "Column::UserId",
        to = "super::user::Column::Id"
    )]
    User,
}

impl Related<super::user::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::User.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
