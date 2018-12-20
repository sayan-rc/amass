import amass
from amass.commands import CommonArgs, Arg


class Command(amass.commands.DjangoCommand):
	usage = CommonArgs("""
			Delete gateway from AMASS
			""",
			[
				Arg("gateway", "Name of gateway to delete"),
			], [])

	def __init__(self):
		amass.commands.Command.__init__(self)
		self.file = __file__

	def run(self, config, args):
		arg_vals = self.parse_args(args)
		[source_gateway] = self.get_django_models(config, "SourceGateway")
		gw = None
		try:
			gw = source_gateway.objects.get(name=arg_vals["gateway"])
		except:
			amass.abort("Gateway '%s' does not exist" % arg_vals["gateway"])

		try:
			gw.delete()
			print "Gateway '%s' sucessfully deleted" % arg_vals["gateway"]
		except Exception as e:
			amass.abort("Problem deleting gateway '%s': %s" % (arg_vals["gateway"], str(e)))