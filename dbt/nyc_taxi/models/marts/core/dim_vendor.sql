select
    toUInt16(vendor_id) as vendor_id,
    vendor_name
from {{ ref('vendor') }}
