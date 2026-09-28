-- 한 주문에 진행 중이거나 성공한 결제는 하나뿐이라는 업무 규칙을 DB 제약으로 표현한다.
--
-- PENDING 을 포함하는 이유가 중요하다. 컨슈머는 PG 를 호출하기 **전에** PENDING 행을 먼저 넣는다.
-- 그래야 같은 주문이 동시에 두 번 들어와도 진 쪽이 여기서 걸려 외부 결제 API 를 부르지 않는다.
-- COMPLETED 만 막았다면 둘 다 조회를 통과해 결제가 두 번 일어난 뒤에야 한 건이 거부된다 — 돈은 이미 나간 뒤다.
--
-- FAILED 는 빠져 있다. 거절은 여러 건 쌓일 수 있어야 한다. 다른 카드로 다시 시도하는 흐름이 막히면 안 된다.
--
-- JPA 의 @Table(uniqueConstraints) 로는 WHERE 절을 표현할 수 없어 여기서 만든다.
-- ddl-auto 가 테이블을 만든 뒤 실행되도록 defer-datasource-initialization 을 켜 두었다.
drop index if exists uk_payments_completed_order;

create unique index if not exists uk_payments_open_order
	on payments (order_id)
	where status in ('PENDING', 'COMPLETED');

-- Hibernate 는 @Enumerated(STRING) 컬럼에 값 목록을 CHECK 제약으로 만들어 둔다.
-- 그런데 `ddl-auto: update` 는 **이미 있는 CHECK 제약을 갱신하지 않는다.**
-- enum 에 값을 추가하면(PENDING 이 그랬다) 기존 DB 에서만 새 값이 거부된다. 그래서 여기서 다시 세운다.
-- 이 파일은 테스트에서도 실행되므로, enum 에 값을 추가하고 아래 목록을 빠뜨리면 테스트가 잡는다(SchemaEnumCheckTest).
-- 대신 목록은 enum 과 손으로 맞춰야 한다. 근본 해결은 Flyway 같은 마이그레이션 도구다.
alter table payments drop constraint if exists payments_status_check;
alter table payments add constraint payments_status_check
	check (status in ('PENDING', 'COMPLETED', 'FAILED'));

alter table orders drop constraint if exists orders_status_check;
alter table orders add constraint orders_status_check
	check (status in ('CREATED', 'PAID', 'PAYMENT_FAILED'));

alter table failed_events drop constraint if exists failed_events_status_check;
alter table failed_events add constraint failed_events_status_check
	check (status in ('PENDING', 'RETRIED', 'RESOLVED', 'IGNORED'));

-- failed_events 유니크 키에 컨슈머 그룹을 넣는다. order.events 는 두 그룹이 읽어서
-- 한 레코드가 두 그룹 모두에서 실패하면 좌표가 같은 DLT 레코드가 2건 온다. 그룹이 없으면 두 번째가 버려진다.
-- 옛 3컬럼 제약은 이름이 DB 마다 다르다. `update` 로 만든 DB 는 Hibernate 가 붙인 UK<해시>,
-- create 로 만든 DB(테스트)는 Postgres 가 붙인 failed_events_original_topic_..._key 다. 그래서 컬럼 구성으로 찾아 지운다.
-- 본문을 $$ 가 아니라 작은따옴표로 감싼 이유: Spring 의 ScriptUtils 는 달러 인용을 몰라 본문 안의 ; 에서 문장을 자른다.
-- 작은따옴표 안의 ; 는 건너뛴다. 대신 본문 안의 따옴표는 '' 로 두 번 쓴다.
do '
declare c text;
begin
	for c in
		select conname from pg_constraint
		where conrelid = ''failed_events''::regclass and contype = ''u''
			and (select array_agg(attname::text order by attname) from pg_attribute where attrelid = conrelid and attnum = any(conkey))
				= array[''original_offset'', ''original_partition'', ''original_topic'']
	loop
		execute format(''alter table failed_events drop constraint %I'', c);
	end loop;
end';
-- NULLS NOT DISTINCT: 그룹 헤더가 없는 레코드(DeadLetterPublishingRecoverer 가 아닌 곳에서 넣은 것)도
-- 재수신은 한 건으로 합친다. 기본(NULL 끼리 다름)이면 DLT 를 다시 읽을 때마다 행이 늘어난다. PostgreSQL 15+.
-- @UniqueConstraint 로는 이 옵션을 표현할 수 없어 여기서 다시 세운다.
alter table failed_events drop constraint if exists uk_failed_events_original_record;
alter table failed_events add constraint uk_failed_events_original_record
	unique nulls not distinct (original_topic, original_partition, original_offset, original_consumer_group);
