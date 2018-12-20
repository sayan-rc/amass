import amass
from amass.commands import CommonArgs, Arg


class Command(amass.commands.DjangoCommand):
	usage = CommonArgs("""
			Add a new gateway error type for this type of gateway
			""",
			[
				Arg("gateway type", "Name of gateway type to add config parameter to"),
				Arg("error", "Name of gateway error type"),
				Arg("regex", "Regular expression that captures errors of this type"),
			], [])

	def __init__(self):
		amass.commands.Command.__init__(self)
		self.file = __file__

	def run(self, config, args):
		arg_vals = self.parse_args(args)
		[source_gateway_type, source_gateway_error] = self.get_django_models(config, "SourceGatewayType", "SourceGatewayError")
		gw = None
		try:
			gw_type = source_gateway_type.objects.get(type=arg_vals["gateway type"])
		except:
			amass.abort("'%s' is not a known gateway type" % arg_vals["gateway type"])

		try:
			source_gateway_error.objects.create(gateway_type=gw_type, error=arg_vals["error"], regex=arg_vals["regex"])
			print "Error type '%s' sucessfully added for gateway '%s'" % (arg_vals["error"], arg_vals["gateway type"])
		except Exception as e:
			amass.abort("Problem adding gateway error '%s' for gateway '%s': %s" % (arg_vals["error"], arg_vals["gateway type"], str(e)))
