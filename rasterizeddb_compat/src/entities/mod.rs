//! sea-orm entities mirroring a representative slice of the `pms` schema.
//!
//! The slice is chosen to cover the feature surface an ORM leans on: UUID
//! primary keys, composite primary keys, nullable columns, timestamps with
//! time zone, fixed-point money, enum-backed small integers, and the three
//! relation shapes (`belongs_to`, `has_many`, many-to-many through a link
//! table). JSON columns from the original schema are omitted by design.

pub mod domain;
pub mod domain_member;
pub mod invoice;
pub mod role;
pub mod subscription;
pub mod user;
pub mod user_role;
pub mod user_session;

pub mod prelude {
    pub use super::domain::Entity as Domain;
    pub use super::domain_member::Entity as DomainMember;
    pub use super::invoice::Entity as Invoice;
    pub use super::role::Entity as Role;
    pub use super::subscription::Entity as Subscription;
    pub use super::user::Entity as User;
    pub use super::user_role::Entity as UserRole;
    pub use super::user_session::Entity as UserSession;
}
