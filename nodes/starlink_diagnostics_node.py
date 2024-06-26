#!/usr/bin/python3

import rospy
from collections.abc import MutableMapping
from diagnostic_msgs.msg import DiagnosticArray, DiagnosticStatus, KeyValue
from starlink_grpc.get_starlink_stats import get_diagnostics

def flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def publish_diagnostic_message():
    # Initialize ROS node
    rospy.init_node('diagnostic_publisher')
    
    # Create ROS publisher for diagnostic array message
    pub = rospy.Publisher('/diagnostics', DiagnosticArray, queue_size=10)

    while not rospy.is_shutdown():
        # Create diagnostic array message
        diag_array = DiagnosticArray()
        diag_array.header.stamp = rospy.Time.now()

        try:
            diagnostic_data = get_diagnostics("192.168.100.1")

            # Create diagnostic status message for each key-value pair in dictionary
            for key, value in diagnostic_data.items():
                # Create diagnostic status message and set its values
                diag_status = DiagnosticStatus()
                diag_status.name = 'Starlink'
                diag_status.hardware_id = (value.get('device_info',{}).get('id'))
                diag_status.message = value.get('message', '')
                diag_status.level = value.get('level', DiagnosticStatus.OK)
                diag_status.values = []
                value = flatten(value)
                # Create key-value messages for each item in the subdictionary
                for k, v in value.items():
                    if k != 'level' and k != 'message':
                        key_value = KeyValue()
                        key_value.key = k
                        key_value.value = str(v)
                        diag_status.values.append(key_value)

                # Add diagnostic status message to diagnostic array
                diag_array.status.append(diag_status)

            # Publish diagnostic array message
            diag_array.header.stamp = rospy.Time.now()
            pub.publish(diag_array)
        except:
            rospy.loginfo("Connection to starlink dish failed. Re-trying... ")

        rospy.sleep(1)

if __name__ == '__main__':
    # diagnostic_data = {'example1': {'level': DiagnosticStatus.OK, 'message': 'This is an example diagnostic message.', 'value1': 10, 'value2': 20},
    #                   'example2': {'level': DiagnosticStatus.ERROR, 'message': 'This is another example diagnostic message.', 'value1': 30, 'value2': 40}}
    # diagnostic_data = get_diagnostics("192.168.100.1")
   
    publish_diagnostic_message()

