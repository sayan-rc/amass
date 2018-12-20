import amass
from amass.commands import CommonArgs, Arg


class Command(amass.commands.DjangoCommand):
	usage = CommonArgs("""
			Add a new gateway
			""",
			[
				Arg("gateway", "Name of gateway to add"),
				Arg("type", "Type of gateway"),
			], [])

	def __init__(self):
		amass.commands.Command.__init__(self)
		self.file = __file__

	def run(self, config, args):
		arg_vals = self.parse_args(args)
		[source_gateway, source_gateway_type] = self.get_django_models(config, "SourceGateway", "SourceGatewayType")
		gw_type = None
		try:
			gw_type = source_gateway_type.objects.get(type=arg_vals["type"])
		except:
			amass.abort("'%s' is not a known gateway type" % arg_vals["type"])

		try:
			source_gateway.objects.create(name=arg_vals["gateway"], type=gw_type)
			print "Gateway '%s' sucessfully added" % arg_vals["gateway"]
		except Exception as e:
			amass.abort("Problem adding gateway '%s': %s" % (arg_vals["gateway"], str(e)))
