//! The outbox row that carries a deletion acknowledgement, and the re-arm of a row whose
//! publication the producer did not see (XR-021 CONTRACTS.md R5-11 rules 1 and 2).

use std::time::Duration;

use ratatoskr_event_envelope::EventPayload as _;
use ratatoskr_identifiers::{EntityRef, TenantRef};
use ratatoskr_knowledge::test_support::TestDatabase;
use ratatoskr_knowledge::{
    DeletionAcknowledgement, EnqueueOutcome, EventCause, EventOutbox, OutboxSubject,
};
use ratatoskr_operation_contracts::{
    DeletionAckFamily, DeletionAckOutcome, DeletionAckRefusal, KnowledgeDeletionAcknowledged,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const OWNER: &str = "user:018f0000-0000-7000-8000-000000000005";
const TOMBSTONE_EVENT: &str = "018f0000-0000-7000-8000-000000000f01";
const SUBJECT: &str = "ai_conversation:018f0000-0000-7000-8000-000000000403";
const CORRELATION: &str = "operation:018f0000-0000-7000-8000-000000000f02";
const REARM: Duration = Duration::from_mins(1);

fn cause() -> Result<EventCause, Box<dyn std::error::Error>> {
    Ok(EventCause {
        event_id: uuid::Uuid::parse_str(TOMBSTONE_EVENT)?,
        correlation_id: EntityRef::parse(CORRELATION)?,
    })
}

fn applied() -> Result<DeletionAcknowledgement, Box<dyn std::error::Error>> {
    let owner: TenantRef = OWNER.parse()?;
    Ok(DeletionAcknowledgement::applied(
        cause()?,
        DeletionAckFamily::AiArchive,
        owner,
        EntityRef::parse(SUBJECT)?,
    ))
}

async fn age_publication(database: &TestDatabase, interval: &str) -> Result<(), sqlx::Error> {
    sqlx::query(&format!(
        "update knowledge.event_outbox set published_at = now() - interval '{interval}'"
    ))
    .execute(database.database.pool())
    .await?;
    Ok(())
}

async fn outbox_rows(database: &TestDatabase) -> Result<(i64, i64), sqlx::Error> {
    sqlx::query_as(
        "select count(*), count(*) filter (where published_at is null)
         from knowledge.event_outbox",
    )
    .fetch_one(database.database.pool())
    .await
}

#[test]
fn the_deletion_acknowledgement_is_a_published_subject() {
    let subject = OutboxSubject::parse("knowledge.deletion.acknowledged.v1");
    assert_eq!(subject, Some(OutboxSubject::DeletionAcknowledged));
    assert_eq!(
        OutboxSubject::DeletionAcknowledged.as_str(),
        KnowledgeDeletionAcknowledged::EVENT_TYPE
    );
}

#[tokio::test]
async fn an_acknowledgement_is_stored_and_published_as_a_valid_envelope() -> TestResult {
    let database = TestDatabase::create().await?;
    let outbox = EventOutbox::new(&database.database);

    assert_eq!(
        outbox.acknowledge_deletion(applied()?, REARM).await?,
        EnqueueOutcome::Inserted
    );

    let rows = outbox.claim_unpublished(10).await?;
    assert_eq!(rows.len(), 1);
    let envelope = rows[0].envelope()?;
    assert_eq!(
        envelope.event_type.to_wire(),
        "knowledge.deletion.acknowledged.v1"
    );
    assert_eq!(envelope.producer.as_str(), "ratatoskr-knowledge");
    assert_eq!(envelope.event_id.0, rows[0].outbox_id);
    assert_eq!(envelope.aggregate_id.to_wire(), SUBJECT);
    assert_eq!(envelope.correlation_id.to_wire(), CORRELATION);
    assert_eq!(
        envelope.causation_id.as_ref().map(EntityRef::to_wire),
        Some(format!("event:{TOMBSTONE_EVENT}"))
    );
    assert_eq!(
        envelope.tenant_id.map(|tenant| tenant.to_string()),
        Some(OWNER.to_owned())
    );
    let payload = envelope.payload_as::<KnowledgeDeletionAcknowledged>()?;
    payload.validate()?;
    assert_eq!(payload.acknowledged_event_id.0.to_string(), TOMBSTONE_EVENT);
    assert_eq!(payload.family, DeletionAckFamily::AiArchive);
    assert_eq!(payload.owner.to_string(), OWNER);
    assert_eq!(payload.subject.to_wire(), SUBJECT);
    assert_eq!(payload.outcome, DeletionAckOutcome::Applied);
    assert_eq!(payload.refusal, None);
    let dedupe: String = sqlx::query_scalar("select dedupe_key from knowledge.event_outbox")
        .fetch_one(database.database.pool())
        .await?;
    assert_eq!(dedupe, format!("deletion_ack:{TOMBSTONE_EVENT}"));
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn a_foreign_subject_is_acknowledged_as_refused() -> TestResult {
    let database = TestDatabase::create().await?;
    let outbox = EventOutbox::new(&database.database);
    let refused = DeletionAcknowledgement::foreign_subject(
        cause()?,
        DeletionAckFamily::Social,
        OWNER.parse()?,
        EntityRef::parse("social_source:018f0000-0000-7000-8000-000000000201")?,
    );

    outbox.acknowledge_deletion(refused, REARM).await?;

    let payload = outbox.claim_unpublished(10).await?[0]
        .envelope()?
        .payload_as::<KnowledgeDeletionAcknowledged>()?;
    assert_eq!(payload.family, DeletionAckFamily::Social);
    assert_eq!(payload.outcome, DeletionAckOutcome::Refused);
    assert_eq!(payload.refusal, Some(DeletionAckRefusal::ForeignSubject));
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn a_repeat_re_arms_only_a_row_published_longer_ago_than_the_window() -> TestResult {
    let database = TestDatabase::create().await?;
    let outbox = EventOutbox::new(&database.database);

    assert_eq!(
        outbox.acknowledge_deletion(applied()?, REARM).await?,
        EnqueueOutcome::Inserted
    );
    assert_eq!(
        outbox.acknowledge_deletion(applied()?, REARM).await?,
        EnqueueOutcome::Unchanged,
        "an unpublished row is already on its way"
    );
    let row = outbox.claim_unpublished(10).await?.remove(0);
    outbox.mark_published(row.outbox_id).await?;

    assert_eq!(
        outbox.acknowledge_deletion(applied()?, REARM).await?,
        EnqueueOutcome::Unchanged,
        "a row published a moment ago is not produced again"
    );
    assert_eq!(outbox_rows(&database).await?, (1, 0));

    age_publication(&database, "30 seconds").await?;
    assert_eq!(
        outbox.acknowledge_deletion(applied()?, REARM).await?,
        EnqueueOutcome::Unchanged,
        "inside the window"
    );

    age_publication(&database, "2 minutes").await?;
    assert_eq!(
        outbox.acknowledge_deletion(applied()?, REARM).await?,
        EnqueueOutcome::Rearmed
    );
    assert_eq!(
        outbox_rows(&database).await?,
        (1, 1),
        "the same row is unpublished again"
    );
    let rearmed = outbox.claim_unpublished(10).await?.remove(0);
    assert_eq!(
        rearmed.outbox_id, row.outbox_id,
        "the same id, so the same Nats-Msg-Id"
    );

    assert_eq!(
        outbox.acknowledge_deletion(applied()?, REARM).await?,
        EnqueueOutcome::Unchanged,
        "a re-armed row is unpublished again"
    );
    database.cleanup().await?;
    Ok(())
}
