from ual.influx import sensors
from ual.influx.influx_buckets import InfluxBuckets
from ual.influx.sensors import SensorSource


def test_sensor_source():
    sensor_source = SensorSource(bucket=InfluxBuckets.UAL_MINUTE_CALIBRATION_BUCKET,
                                 sensor=sensors.Sensors.UAL_3)
    assert sensor_source.get_bucket() == "ual-minute-calibration"
    assert sensor_source.get_sensor() == "ual-3"

def test_sensor_source_from_strings():
    sensor_source = SensorSource.from_strings("ual-minute-calibration", "ual-3")
    assert sensor_source.get_bucket() == "ual-minute-calibration"
    assert sensor_source.get_sensor() == "ual-3"