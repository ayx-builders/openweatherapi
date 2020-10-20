import datetime
import json

import AlteryxPythonSDK as Sdk
import xml.etree.ElementTree as Et
import requests
from obj_query import AyxDataMap, FieldType, Query


class AyxPlugin:
    def __init__(self, n_tool_id: int, alteryx_engine: object, output_anchor_mgr: object):
        # Default properties
        self.n_tool_id: int = n_tool_id
        self.alteryx_engine: Sdk.AlteryxEngine = alteryx_engine
        self.output_anchor_mgr: Sdk.OutputAnchorManager = output_anchor_mgr
        self.label = "OpenWeather (" + str(n_tool_id) + ")"

        # Custom properties
        self.ApiKey: str = None
        self.Lon: float = None
        self.Lat: float = None
        self.Endpoint: str = None
        self.Units: str = None

    def pi_init(self, str_xml: str):
        xml_parser = Et.fromstring(str_xml)
        self.ApiKey = self.parse_tag(xml_parser, 'Key', True)

        lon_str = self.parse_tag(xml_parser, 'Longitude', False)
        lon_float = string_to_float(lon_str)
        if lon_float is None:
            self.display_error_msg('Longitude is not a valid number')
        self.Lon = lon_float

        lat_str = self.parse_tag(xml_parser, 'Latitude', False)
        lat_float = string_to_float(lat_str)
        if lat_float is None:
            self.display_error_msg('Latitude is not a valid number')
        self.Lat = lat_float

        units = self.parse_tag(xml_parser, 'Units', False)
        if units is None:
            self.display_error_msg('Units was not selected')
        self.Units = units

        self.Endpoint = self.parse_tag(xml_parser, 'Endpoint', True)

        # Getting the output anchor from Config.xml by the output connection name
        self.Output = self.output_anchor_mgr.get_output_anchor('Output')
        self.WeatherConditionCodes = self.output_anchor_mgr.get_output_anchor('Weather Condition Codes')

    def pi_add_incoming_connection(self, str_type: str, str_name: str) -> object:
        raise NotImplementedError('unexpected; this is an input tool')

    def pi_add_outgoing_connection(self, str_name: str) -> bool:
        return True

    def pi_push_all_records(self, n_record_limit: int) -> bool:
        if self.Endpoint == 'Current':
            return self.import_current_forecast()

        self.display_error_msg(f"Unsupported endpoint {self.Endpoint}")
        return False

    def import_current_forecast(self) -> bool:
        data_mapper = AyxDataMap(self.alteryx_engine, self.label, {
            ("Temperature", FieldType.Decimal): Query().get('main').get('temp').finalize(),
            ("Feels Like", FieldType.Decimal): Query().get('main').get('feels_like').finalize(),
            ("Min Temperature", FieldType.Decimal): Query().get('main').get('temp_min').finalize(),
            ("Max Temperature", FieldType.Decimal): Query().get('main').get('temp_max').finalize(),
            ("Atmospheric Pressure", FieldType.Integer): Query().get('main').get('pressure').finalize(),
            ("Humidity", FieldType.Integer): Query().get('main').get('humidity').finalize(),
            ("Visibility", FieldType.Integer): Query().get('visibility').finalize(),
            ("Wind Speed", FieldType.Decimal): Query().get('wind').get('speed').finalize(),
            ("Wind Direction", FieldType.Decimal): Query().get('wind').get('deg').finalize(),
            ("Percentage of Cloudiness", FieldType.Decimal): Query().get('clouds').get('all').custom(divide_by_hundred).finalize(),
            ("Timestamp", FieldType.Datetime): Query().get('dt').custom(unix_timestamp_to_datetime).finalize(),
            ("Sunrise", FieldType.Datetime): Query().get('sys').get('sunrise').custom(unix_timestamp_to_datetime).finalize(),
            ("Sunset", FieldType.Datetime): Query().get('sys').get('sunset').custom(unix_timestamp_to_datetime).finalize(),
            ("TZ Shift", FieldType.Integer): Query().get('timezone').finalize(),
            ("City ID", FieldType.Integer): Query().get('id').finalize(),
            ("City Name", FieldType.String): Query().get('name').finalize(),
        })

        condition_code_mapper = AyxDataMap(self.alteryx_engine, self.label, {
            ("Condition ID", FieldType.Integer): Query().get('id').finalize(),
            ("Condition Name", FieldType.String): Query().get('main').finalize(),
            ("Condition Description", FieldType.String): Query().get('description').finalize(),
            ("Condition Icon", FieldType.String): Query().get('icon').custom(icon_to_url).finalize(),
        })

        info = data_mapper.Info
        self.Output.init(info)
        condition_code_info = condition_code_mapper.Info
        self.WeatherConditionCodes.init(condition_code_info)

        if self.alteryx_engine.get_init_var(self.n_tool_id, 'UpdateOnly') == 'True':
            self.Output.close()
            return True

        api_key = self.alteryx_engine.decrypt_password(self.ApiKey)
        url = f"https://api.openweathermap.org/data/2.5/weather?lat={self.Lat}&lon={self.Lon}&appid={api_key}&units={self.Units}"
        response = requests.get(url)
        if response.status_code != 200:
            self.display_error_msg(response.text)
            return False

        response_obj = json.loads(response.text)
        blob = data_mapper.transfer(response_obj)
        self.Output.push_record(blob)

        for condition_code in response_obj['weather']:
            condition_code_blob = condition_code_mapper.transfer(condition_code)
            self.WeatherConditionCodes.push_record(condition_code_blob)

        self.Output.close()
        self.WeatherConditionCodes.close()

        return True

    def pi_close(self, b_has_errors: bool):
        return

    def display_error_msg(self, msg_string: str):
        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error, msg_string)

    def display_info_msg(self, msg_string: str):
        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.info, msg_string)

    def parse_tag(self, config_obj, tag_name, is_required):
        element = config_obj.find(tag_name)
        if element is not None and element.text is not None and element.text != '':
            return element.text
        if is_required:
            self.display_error_msg(f"Missing {tag_name}")
        return None


def string_to_float(value_str):
    try:
        return float(value_str)
    except:
        return None


def unix_timestamp_to_datetime(value):
    return datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=value)


def divide_by_hundred(value):
    return value/100


def icon_to_url(value):
    return f"http://openweathermap.org/img/wn/{value}@2x.png"
