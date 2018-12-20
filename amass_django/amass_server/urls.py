from django.conf.urls import include, url

from rest_framework import routers
from . import views

appname = 'amass_server'

router = routers.DefaultRouter()
router.register(r'sourcegatewaytype', views.SourceGatewayType_List)

# Wire up our API using automatic URL routing.
# Additionally, we include login URLs for the browsable API.
urlpatterns = [
    url(r'^', include(router.urls)),
    url(r'^api-auth/', include('rest_framework.urls', namespace='rest_framework'))
]
#urlpatterns = [
 #   url(r'^$', views.index, name='index'),

    #url(r'^$', views.IndexView.as_view(), name='index'),
    #url(r'^(?P<pk>[0-9]+)/$', views.DetailView.as_view(), name='detail'),
#]
