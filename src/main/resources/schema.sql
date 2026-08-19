-- 한 주문에 성공한 결제는 하나뿐이라는 업무 규칙을 DB 제약으로 표현한다.
--
-- 부분 인덱스인 이유: 거절(FAILED)은 여러 건 쌓일 수 있어야 한다. 다른 카드로 다시 시도하는 흐름이
-- 막히면 안 된다. 성공한 결제만 하나로 제한한다.
--
-- JPA 의 @Table(uniqueConstraints) 로는 WHERE 절을 표현할 수 없어 여기서 만든다.
-- ddl-auto 가 테이블을 만든 뒤 실행되도록 defer-datasource-initialization 을 켜 두었다.
create unique index if not exists uk_payments_completed_order
	on payments (order_id)
	where status = 'COMPLETED';
