import AlteryxPythonSDK as Sdk
import xml.etree.ElementTree as Et


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

        self.Endpoint = self.parse_tag(xml_parser, 'Endpoint', True)

        # Getting the output anchor from Config.xml by the output connection name
        self.Output = self.output_anchor_mgr.get_output_anchor('Output')

    def pi_add_incoming_connection(self, str_type: str, str_name: str) -> object:
        raise NotImplementedError('unexpected; this is an input tool')

    def pi_add_outgoing_connection(self, str_name: str) -> bool:
        return True

    def pi_push_all_records(self, n_record_limit: int) -> bool:

        return False

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

