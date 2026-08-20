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
