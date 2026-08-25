use chrono::{DateTime, FixedOffset};
use rust_decimal::Decimal;
use sea_orm::entity::prelude::*;
use uuid::Uuid;

/// Trimmed from the production `users` table: the billing-address block is
/// dropped as repetitive, everything structurally distinct is kept.
#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "users")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,
    pub name: String,
    pub email: String,
    pub password: String,
    pub is_service_account: bool,
    pub is_privilaged: bool,
    pub domain_id: Uuid,
    pub billing_setup: bool,
    pub is_legal_entity: bool,
    pub legal_entity_name: Option<String>,
    pub country: Option<String>,
    pub vat_rate: Decimal,
    pub email_verified_at: Option<DateTime<FixedOffset>>,
    pub username: Option<String>,
    pub approved_at: Option<DateTime<FixedOffset>>,
    pub created_at: DateTime<FixedOffset>,
    pub updated_at: DateTime<FixedOffset>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::domain::Entity",
        from = "Column::DomainId",
        to = "super::domain::Column::Id"
    )]
    Domain,
    #[sea_orm(has_many = "super::user_role::Entity")]
    UserRole,
    #[sea_orm(has_many = "super::user_session::Entity")]
    UserSession,
    #[sea_orm(has_many = "super::subscription::Entity")]
    Subscription,
    #[sea_orm(has_many = "super::invoice::Entity")]
    Invoice,
}

impl Related<super::domain::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Domain.def()
    }
}

impl Related<super::user_session::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::UserSession.def()
    }
}

impl Related<super::subscription::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Subscription.def()
    }
}

impl Related<super::invoice::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Invoice.def()
    }
}

/// Many-to-many: users are related to roles *through* the link table.
impl Related<super::role::Entity> for Entity {
    fn to() -> RelationDef {
        super::user_role::Relation::Role.def()
    }

    fn via() -> Option<RelationDef> {
        Some(super::user_role::Relation::User.def().rev())
    }
}

impl ActiveModelBehavior for ActiveModel {}
