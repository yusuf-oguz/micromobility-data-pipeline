# Kibana Dashboard Setup

Kibana UI: http://localhost:5601
Login: elastic / elastic123

## 1. Create Index Pattern

1. Stack Management → Index Patterns → Create index pattern
2. Name: `scooter-alerts*`
3. Time field: `timestamp`
4. Save

## 2. Scooter Alerts Map

Menu → Maps → Create map

- Add layer → Documents → Index: `scooter-alerts*`
- Display field: `anomaly_type`
- Geo field: `location` (geo_point)
- Style by `anomaly_type` (topple=red, critical_battery=orange, gps_loss=yellow, motor_fault=purple)

## 3. Live Alerts Dashboard

Menu → Dashboard → Create dashboard → Add panels:

### Panel 1: Alert count by type (Pie chart)
- Index: scooter-alerts*
- Aggregation: Count, Split slices by: Terms on `anomaly_type`

### Panel 2: Battery level over time (Line chart)
- Index: scooter-alerts*
- Aggregation: Average of `battery_pct`, X-axis: Date histogram on `timestamp`

### Panel 3: Alerts per scooter (Data table)
- Index: scooter-alerts*
- Aggregation: Count, Split rows by: Terms on `scooter_id`, size 20

### Panel 4: Fault codes (Tag cloud)
- Index: scooter-alerts*
- Tags: Terms on `fault_code`

## 4. Auto-refresh

Dashboard → Options → Refresh every: 10s
Time range: Last 30 minutes

## 5. Export / Import

To export dashboard for repo:
Stack Management → Saved Objects → Export → Select dashboard + dependencies → Export

To import:
Stack Management → Saved Objects → Import → select ndjson file
