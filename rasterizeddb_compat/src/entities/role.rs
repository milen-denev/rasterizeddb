use chrono::{DateTime, FixedOffset};
use sea_orm::entity::prelude::*;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "roles")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,
    pub name: String,
    pub domain_id: Option<Uuid>,
    pub date_added: DateTime<FixedOffset>,
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
}

impl Related<super::domain::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Domain.def()
    }
}

impl Related<super::user::Entity> for Entity {
    fn to() -> RelationDef {
        super::user_role::Relation::User.def()
    }

    fn via() -> Option<RelationDef> {
        Some(super::user_role::Relation::Role.def().rev())
    }
}

impl ActiveModelBehavior for ActiveModel {}
