# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin

from .models import * 

# Register your models here.
admin.site.register(SourceGateway)
admin.site.register(SourceGatewayType)
admin.site.register(SourceConfig)
admin.site.register(Resource)
admin.site.register(SourceResource)
admin.site.register(SourceGatewayError)

