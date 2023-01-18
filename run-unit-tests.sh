# from pycyphal
## from _ip/endpoint_mapping
pytest -k _unittest_udp_endpoint_mapping --pdb
## from _ip/v4
pytest -k _unittest_udp_socket_factory_v4 --pdb
## from frame
pytest -k _unittest_udp_frame_compile --pdb
pytest -k _unittest_udp_frame_parse --pdb
## from tracer
pytest -k _unittest_udp_tracer --pdb



# from tests
## from ip/link_layer
pytest -k _unittest_encode_decode_null --pdb
pytest -k _unittest_encode_decode_loop --pdb
pytest -k _unittest_encode_decode_ethernet --pdb
pytest -k _unittest_find_devices --pdb
pytest -k _unittest_sniff --pdb
pytest -k _unittest_sniff_errors --pdb
## ip/v4
pytest -k _unittest_socket_factory --pdb
pytest -k _unittest_sniffer --pdb
## input_session
pytest -k _unittest_udp_input_session_uniframe --pdb
pytest -k _unittest_udp_input_session_multiframe --pdb
## output_session
pytest -k _unittest_udp_output_session --pdb
pytest -k _unittest_output_session_no_listener --pdb
## udp
pytest -k _unittest_udp_transport_ipv4 --pdb
pytest -k  _unittest_udp_transport_ipv4_capture --pdb
