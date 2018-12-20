# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.http import HttpResponse
from django.shortcuts import render

from rest_framework import viewsets, status
from rest_framework.views import APIView
from rest_framework.permissions import IsAuthenticated, IsAuthenticatedOrReadOnly, AllowAny
from rest_framework.response import Response
from amass_server.models import *
from amass_server.serializers import *

# Create your views here.

def index(request):
	return HttpResponse("AMASS index")

#class SourceGatewayType_List(APIView):
class SourceGatewayType_List(viewsets.ModelViewSet):
	queryset = SourceGatewayType.objects.all().order_by('-type')
	serializer_class = SourceGatewayTypeSerializer
    #permission_classes = (IsAuthenticatedOrReadOnly,)
#    def get(self, request, format=None):
#        objects = SourceGatewayType.objects.all()
#        serializer = SourceGatewayTypeSerializer(objects, many=True)
#        return Response(serializer.data)
#
#    def post(self, request, format=None):
#        serializer = SourceGatewayTypeSerializer(data=request.data)
#        if serializer.is_valid():
#            serializer.save()
#            return Response(serializer.data, status=status.HTTP_201_CREATED)
#        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
