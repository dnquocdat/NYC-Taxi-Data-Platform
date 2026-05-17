select
    toUInt16(payment_type_id) as payment_type_id,
    payment_type_name
from {{ ref('payment_type') }}
