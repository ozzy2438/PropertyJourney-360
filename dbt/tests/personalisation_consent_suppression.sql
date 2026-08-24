select *
from {{ ref('mart_personalisation_features') }}
where not eligible_for_personalisation
  and (
      views_7d != 0
      or views_30d != 0
      or saved_properties_30d != 0
      or enquiries_30d != 0
      or array_length(preferred_suburbs) != 0
      or preferred_property_type is not null
      or observed_price_min is not null
      or observed_price_max is not null
      or last_activity_at is not null
      or engagement_frequency != 0.0
      or engagement_score != 0
  )
