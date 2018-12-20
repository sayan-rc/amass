from amass_server.models import *
from rest_framework import serializers

class SourceGatewayTypeSerializer(serializers.ModelSerializer):
    class Meta:
        model = SourceGatewayType
        fields = ('type',)

