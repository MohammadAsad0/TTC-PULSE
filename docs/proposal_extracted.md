# Proposal Extracted Text

Source: `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/proposal.pdf`
Pages: 3

## Page 1

Interactive Visualization & Analytics of TTC Ridership and
Service Dynamics
Awais Aziz
Electrical Engineering and Computer
Science
York University
Toronto, Canada
azizawai@yorku.ca
Omkumar Patel
Electrical Engineering and Computer
Science
York University
Toronto, Canada
patelom@yorku.ca
Muhammad Asad
Electrical Engineering and Computer
Science
York University
Toronto, Canada
mohdasad@yorku.ca
Abstract
Urban public transit systems generate large volumes of operational
and incident data, yet extracting meaningful insights about service
reliability remains challenging. In the Toronto Transit Commis-
sion (TTC) network, delays, headway irregularities, and disruption
events occur across space and time, but existing performance sum-
maries often rely on aggregate statistics that obscure localized and
recurring reliability issues.
This project proposes an interactive visual analytics framework
for analyzing TTC service reliability using historical delay logs,
static GTFS reference data, and GTFS-Realtime service alerts. The
system identifies recurring spatiotemporal disruption hotspots,
quantifies reliability through frequency, severity, regularity, and
modality, and validates whether live alerts align with historical
problem areas
Hence, the goal is to move beyond static performance metrics
toward a structured, data-driven framework for understanding
where, when, and why service disruptions occur. The resulting
system will support transit planners, researchers, and commuters
by transforming raw transit data streams into actionable reliability
insights and decision-support tools.
Project Repository:TTC-PULSE
1 Motivation & Domain Description
1.1 Data Domain:
This project lies in the domain of urban public transportation ana-
lytics, with a focus on service reliability within the Toronto Transit
Commission (TTC). The project integrates multiple heterogeneous
data sources, summarized in Table 1, to analyze recurring disrup-
tions across space, time, and incident type.
Historical delay logs include route or line context, incident codes,
timestamps, and operational measures such :
•Min Delay:Delay in minutes to the following vehicle
•Min Gap:Scheduled time gap between vehicles
GTFS-Realtime feeds can include trip updates, vehicle positions,
and service alerts; in this project we focus primarily on service
alerts as the real-time disruption layer. Together, these datasets
support spatiotemporal analysis of disruption frequency, severity,
regularity, and cause.
1.2 Goal of the Project:
The goal of this project is to design a scalable and interactive visual
analytics framework for TTC service reliability that transforms raw
Table 1: Datasets used in the project
Dataset Purpose
Historical Delay Logs Incident frequency, severity, gap ir-
regularity, cause analysis
Static GTFS Route/stop geometry and identifier
alignment
GTFS-RT Service Alerts Real-time disruption validation
Traffic Data (optional) Bus hotspot case study
delay logs into structured insights, identifies recurring disruption
hotspots, and validates those patterns using real-time operational
alerts. More specifically, the system is designed to reveal where,
when and why service interruptions occur and how severe or irreg-
ular they become in bus and subway service.
1.3 Motivation
As we know that the urban transit systems are complex and stochas-
tic systems, highly influenced by traffic congestion, infrastructure
constraints, weather, passenger demand, and operational decisions.
Their reliability matters because recurring disruptions affect large
numbers of riders and can propagate across the network. Therefore,
in a rapidly growing city like Toronto, data-driven evaluation is es-
sential for sustainable mobility planning. According to TTC transit
planning data, the system recorded approximately 2.5 million aver-
age weekday boardings in 2024, highlighting the scale of impact
when disruptions recur in specific corridors or stations. Aggre-
gate performance summaries often obscure these localized patterns.
Therefore, structured spatiotemporal analytics are needed to iden-
tify persistent reliability hotspots and support more evidence-based
operational planning.
2 Research Questions
(1) Where and when do TTC service disruptions occur most
frequently, and how do these patterns differ between bus
and subway service?
(2) Which incident types dominate specific routes, stations, or
corridors, and how do they relate to disruption severity and
gap irregularity?
(3) Do live GTFS-Realtime service alerts align with historical dis-
ruption hotspots, and can they be used to validate recurring
reliability patterns?
1

## Page 2

EECS 6414 – Project Proposal TTC
Historical
Delay Logs
Static
GTFS
GTFS-RT
Alerts
Clean &
Normalize
Space-Time
Aggregation
Reliability
Metrics
Alert
Validation
Linked Dashboard
(Map, Heatmap, Timeline, Rankings)
Figure 1: Methodology overview: data integration, hotspot
discovery, alert validation, and linked analytics.
3 Methodology
We propose aTTC Reliability Observatorythat combines historical
TTC bus and subway delay incidents with live GTFS-Realtime ser-
vice alerts to identify recurring spatiotemporal disruption hotspots
and expose them through a linked-view visual analytics dashboard.
The methodology follows a simple pipeline: integrate raw logs and
reference geometry, aggregate incidents across space and time, com-
pute interpretable reliability metrics, and validate them using live
alerts.
3.1 Data Preparation and Integration
As shown in Figure 1, We first ingest historical TTC bus and sub-
way delay records, standardize them into a unified event schema
containing timestamp, mode, route/line, station or stop context,
incident code, delay minutes, and gap minutes and allow the same
aggregation and visualization pipeline to be applied across both
transit modes.
The static GTFS bridges between historical delay records and
GTFS-Realtime. As it provides canonical stop/station geometry
and route structure, enabling consistent spatial joins and loca-
tion normalization. GTFS-RealtimeService Alertscollected over
approximately one month are taken into account. As they explicitly
supports service alerts published by TTC for short-term service
disruptions.
3.2 Spatiotemporal Aggregation and Reliability
Metrics
The core analysis transforms incident-level rows into recurring
“where it breaks” and “when it breaks” patterns.
Spatial units:Our primary plan is to use H3 hexagonal bin-
ning for city-wide hotspot maps. H3 is a hierarchical geospatial
indexing system that supports stable, multi-resolution spatial aggre-
gation. If geocoding proves too noisy, especially for bus incidents
with free-text location descriptions, we fall back to station-level
aggregation for subway and route/segment-level aggregation for
bus using cleaned location strings.
Temporal units:We aggregate incidents by hour-of-day and
day-of-week to detect recurring patterns such as peak-period fail-
ures at specific stations or corridors. We also compute monthly or
seasonal slices to identify longer-term recurring trends.
Reliability metrics:To avoid reducing reliability to a single av-
erage, we define it through four complementary metrics computed
for each mode, route/station, and space-time bin:
• Frequency:incident count per bin, optionally normalized
by time coverage.
• Severity:delay-minute summaries using the median ( 𝑝50)
and upper tail (𝑝90) of Min Delay , distinguishing frequent
minor delays from rarer major failures.
• Regularity:upper-tail gap minutes ( 𝑝90of Min Gap ) as a
proxy for headway irregularity and bunching.
• Modality:the distribution of incident codes within each
hotspot and time window, optionally grouped into broader
categories such as mechanical, operational, passenger-
related, or traffic-related.
For ranking the “top offenders, ” we compute an explainable com-
posite score:
𝑆(𝑢, 𝑡)=𝑤 1𝑧(Freq) +𝑤 2𝑧(Sev 90) +𝑤 3𝑧(Reg) +𝑤 4 Modality,
where 𝑢 denotes a spatial unit (e.g., route, station, or H3 cell) and 𝑡
denotes a time bin. The dashboard always displays the component
metrics alongside this score so rankings remain interpretable.
3.3 Real-Time Validation and Dashboard Design
We test whether “today’s problems match yesterday’s patterns”
by computing a real-time alert–hotspot match score. Historical
hotspots are first defined using top-ranked spatial bins. We then
measure:
•The proportion of live alerts that fall within these hotspots
• The lift relative to a baseline such as randomly assigned or
spatially stratified alert locations.
This fusion connects descriptive analytics with validation: history
discovers recurring patterns, and real-time data checks whether
they recur. Finally the visualization system includes coordinated
multiple views, including hotspot maps, hour×day heatmaps, time-
series trends, cause-signature charts, top-offender rankings, and a
live alert overlay panel. These views are linked through coordinated
interaction: selecting a hotspot on the map filters the heatmap,
timeline, cause chart, and ranking table. The system supports drill-
down from city-wide view to corridor, route/line, and stop/station
level, making reliability visible as a localized spatiotemporal pattern
rather than a single system-wide number.
4 Evaluation
The evaluation of the proposed TTC reliability analytics system
will focus on determining whether the framework can effectively
identify meaningful spatiotemporal disruption patterns and sup-
port actionable insights for transit stakeholders. The evaluation
combines data validation, statistical analysis of reliability metrics,
and experiments that compare historical disruption patterns with
2

## Page 3

EECS 6414 – Project Proposal TTC
real-time service alerts. In addition, scalability and generalizability
of the analytical framework will be assessed.
4.1 Data Validation and Reliability Metric
Evaluation
Before performing analytical evaluation, the integrated datasets
will undergo validation to ensure reliability of the results. This
includes schema consistency checks across historical delay logs,
static GTFS data, and GTFS-Realtime service alerts, as well as anal-
ysis of missing timestamps, incomplete incident descriptions, or
ambiguous location information. Temporal sanity checks will verify
expected operational patterns such as higher disruption frequencies
during peak commuting hours.
The reliability metrics defined in the methodology, frequency of
incidents, severity of delay (median and upper-tail delay minutes),
headway regularity using Min Gap statistics, and incident modality
distributions will be analyzed statistically to determine whether
they capture meaningful service reliability trends. Techniques such
as correlation analysis, variance analysis across routes and time
periods, and peak versus off-peak comparisons will be used to
validate that these indicators reflect real operational patterns in
TTC service disruptions.
4.2 Experiments for Historical Pattern
Validation
To evaluate whether historical disruption patterns can explain or
predict current disruptions, the system will compare historically
identified hotspots with live GTFS-Realtime service alerts. His-
torical hotspots are first detected through spatial and temporal
aggregation of delay incidents. The following experimental metrics
will be used:
• Hit Rate: the proportion of real-time alerts occurring within
historically identified disruption hotspots.
• Lift over Random Baseline: comparison of hotspot predic-
tion accuracy against randomly distributed alert locations.
• Temporal Recurrence: the frequency with which disrup-
tions reappear during previously identified high-risk time
windows.
These experiments evaluate whether the system’s historical an-
alytics provide meaningful predictive or explanatory power for
current service disruptions.
4.3 External Dataset Validation and Scalability
To assess generalizability, the analytical pipeline may be applied to
additional transportation datasets such as traffic congestion data
from the Greater Toronto Area. Integrating traffic information al-
lows evaluation of whether congestion patterns align with identi-
fied bus disruption hotspots and provides further validation of the
analytical framework.
Scalability will be evaluated by examining system performance as
dataset size increases. The framework is designed to scale through
partitioned data storage, space-time aggregation techniques, and
distributed data processing. By measuring runtime and memory
usage as additional historical data and real-time feeds are incor-
porated, the evaluation will demonstrate whether the proposed
approach can support continuous growth in transit data and be
adapted to similar large-scale transit datasets in other cities.
5 Conclusion
This project proposes a scalable and interactive visual analytics
framework for analyzing TTC service reliability using historical
delay data, GTFS reference information, and GTFS-Realtime service
alerts. By integrating these sources, the system identifies recurring
spatiotemporal disruption patterns, quantifies reliability through
interpretable metrics, and validates historical hotspots with real-
time alerts. The resulting platform aims to support transit planners
and researchers by providing data-driven insights into where, when,
and why service disruptions occur.
References
[1] A. Abdi and C. Amrit. 2021. A Review of Travel and Arrival-Time Prediction
Methods on Road Networks: Classification, Challenges and Opportunities.PeerJ
Computer Science7 (Sept. 2021), e689. doi:10.7717/peerj-cs.689
[2] City of Toronto Open Data. 2019. TTC Bus Delay Data. https://open.toronto.ca/
dataset/ttc-bus-delay-data/. Accessed: 2026-03-06.
[3] City of Toronto Open Data. 2019. TTC Subway Delay Data. https://open.toronto
.ca/dataset/ttc-subway-delay-data/. City of Toronto Open Data Portal. Accessed:
2026-03-06.
[4] City of Toronto Open Data. 2021. Traffic Volumes at Intersections for All Modes.
https://data.urbandatacentre.ca/catalogue/city-toronto-traffic-volumes-at-
intersections-for-all-modes. Dataset containing multimodal intersection turning
movement counts including vehicles, cyclists, and pedestrians collected by the
City of Toronto Transportation Services Division. Accessed: 2026-03-06.
[5] City of Toronto Open Data. 2025. Merged GTFS – TTC Routes and Schedules.
https://data.urbandatacentre.ca/fr/catalogue/city-toronto-merged-gtfs-ttc-
routes-and-schedules. Dataset providing GTFS scheduling information including
route definitions, stop patterns, stop locations, and schedules for TTC buses,
streetcars, and subways. Accessed: 2026-03-06.
[6] Luyu Liu, Adam Porr, and Harvey J. Miller. 2024. Measuring the Impacts of
Disruptions on Public Transit Accessibility and Reliability.Journal of Transport
Geography114 (2024), 103769. doi:10.1016/j.jtrangeo.2023.103769
[7] Rick Zhaoju Liu and Amer Shalaby. 2024. Impacts of public transit delays and dis-
ruptions on equity seeking groups in Toronto – A time-expanded graph approach.
Journal of Transport Geography114 (2024), 103763. doi:10.1016/j.jtrangeo.2023.10
3763
[8] Toronto Transit Commission. 2026. TTC GTFS-Realtime Feed Endpoints. https:
//gtfsrt.ttc.ca/. Provides live GTFS-Realtime endpoints including Trip Updates,
Vehicle Positions, and Service Alerts. Accessed: 2026-03-06.
3
