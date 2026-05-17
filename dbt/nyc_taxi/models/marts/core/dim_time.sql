select
    toUInt8(number) as time_id,
    toUInt8(number) as hour_of_day,
    case
        when number between 5 and 11 then 'morning'
        when number between 12 and 16 then 'afternoon'
        when number between 17 and 21 then 'evening'
        else 'night'
    end as day_part
from numbers(24)
