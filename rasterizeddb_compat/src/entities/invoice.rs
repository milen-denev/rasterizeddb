use chrono::{DateTime, FixedOffset};
use rust_decimal::Decimal;
use sea_orm::entity::prelude::*;
use uuid::Uuid;

/// Fixed-point money and date-range filtering: the aggregate-heavy table.
#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "invoice")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,
    pub invoice_number: i64,
    pub subscription_id: Uuid,
    pub domain_id: Uuid,
    pub user_id: Uuid,
    pub billing_period_start: DateTime<FixedOffset>,
    pub billing_period_end: DateTime<FixedOffset>,
    pub issued_at: DateTime<FixedOffset>,
    pub due_at: DateTime<FixedOffset>,
    pub net_amount: Decimal,
    pub vat_amount: Decimal,
    pub total_amount: Decimal,
    pub vat_rate: Decimal,
    pub status: String,
    pub stripe_payment_intent_id: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::user::Entity",
        from = "Column::UserId",
        to = "super::user::Column::Id"
    )]
    User,
    #[sea_orm(
        belongs_to = "super::subscription::Entity",
        from = "Column::SubscriptionId",
        to = "super::subscription::Column::Id"
    )]
    Subscription,
}

impl Related<super::user::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::User.def()
    }
}

impl Related<super::subscription::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Subscription.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
