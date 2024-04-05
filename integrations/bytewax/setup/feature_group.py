import hopsworks

from hsfs.feature import Feature

project = hopsworks.login()
fs = project.get_feature_store()

# Setup the feature groups for the bytewax pipelines
# Price Features
features = [
    Feature(name="cc_num", type="string"),
    Feature(name="timestamp", type="timestamp"),
    Feature(name="min_amount", type="float"),
    Feature(name="max_amount", type="float"),
    Feature(name="mean", type="float")
]

fg = fs.create_feature_group(
    name="transactions_windowed_agg",
    version=1,
    primary_key=["cc_num"],
    event_time="timestamp",
    statistics_config=False,
    online_enabled=True,
)

fg.save(features)