import boto3
import base64

ec2 = boto3.client('ec2')
# mycode = """#!/bin/bash
# mkdir -p /home/ubuntu/testfolder"""


def start_aws_nodes(aws_instance_count, aws_instance_type, aws_spot_price, aws_path_to_rtp, aws_key_name, aws_ami_id):

    startup_code = """#!/bin/bash -l
aws s3 cp s3://mwatest/MWA_FHD_RTP.tar /usr/local/MWA_FHD_RTP.tar
cd /usr/local
tar xpvf MWA_FHD_RTP.tar
export PATH="/usr/local/anaconda2/bin:$PATH"
%s/bin/still.py --server --config_file /usr/local/MWA/RTP/etc/aws_firstpass.cfg &""" % aws_path_to_rtp

    ec2.request_spot_instances(
        SpotPrice=str(aws_spot_price),
        InstanceCount=int(aws_instance_count),
        LaunchSpecification={
            'ImageId': aws_ami_id,
            'UserData': base64.b64encode(startup_code),
            'KeyName': aws_key_name,
            'InstanceType': aws_instance_type,
            'NetworkInterfaces': [{"DeviceIndex": 0, 'SubnetId': 'subnet-61c9c716', 'Groups': ['sg-9d7fe4e5'], "AssociatePublicIpAddress": True}]
        }
    )

#    rc = ec2.create_instances(
#        ImageId=aws_ami_id,
#        MinCount=int(aws_instance_count),
#        MaxCount=int(aws_instance_count),
#        KeyName=aws_key_name,
#        InstanceType=aws_instance_type,
#        UserData=startup_code,
#        NetworkInterfaces=[{"DeviceIndex": 0, 'SubnetId': 'subnet-61c9c716', 'Groups': ['sg-9d7fe4e5'], "AssociatePublicIpAddress": True}]
#    )

#    for instance in rc:
#        instance.wait_until_running()
#        instance.load()
#        print(instance.public_ip_address)
