import amass
import amass.predict
from amass.commands import CommonArgs, Arg, Opt
import re


class Command(amass.commands.DjangoCommand):
	usage = CommonArgs(
		"""
		Generate features for selected gateway
		""",
		[
			Arg("features", "Name of cached feature set to run prediction on"),
			Arg("gateway", "Name of cached gateway features"),
			Arg("startdate", "Start date of jobs in 'YYYY-MM-DD HH:MM:SS' format"),
			Arg("enddate", "End date of jobs in 'YYYY-MM-DD HH:MM:SS' format")
		],
		[
			Opt("refresh-features", "Re-generate all features", "False"),
			Opt("refresh-sources", "Re-generate specified sources (comma separated)", ""),
			Opt("jobid", "Only generate features for specified job", "")
		]
	)

	def __init__(self):
		amass.commands.Command.__init__(self)
		self.file = __file__

	def run(self, config, args):
		arg_vals = self.parse_args(args)
		server_model_names = ["SourceConfig", "Resource", "SourceResource"]
		server_models = self.get_django_models(config, *server_model_names)
		server_config = {}
		for i, model in enumerate(server_models):
			server_config[server_model_names[i]] = model

		startDate = "\'2016-03-01 03:00:00\'"
		endDate = "\'2016-06-01 4:00:00\'"

		features = amass.features.Features(config, server_config)
		features.cache_init()
		refresh_features = self.is_arg_true(arg_vals["refresh-features"])
		refresh_sources = []
		if arg_vals["refresh-sources"] != "":
			refresh_sources = re.split("\s*,\s*", arg_vals["refresh-sources"])
		if not features.load_or_fetch_sources(
			arg_vals["features"],
			arg_vals["gateway"],
			arg_vals["startdate"],
			arg_vals["enddate"], refresh_sources ):
			amass.abort("Unable to fetch source data for features %s" % arg_vals["features"])
		if arg_vals["jobid"] == "":
			features.generate_features(arg_vals["features"], arg_vals["gateway"], refresh_features)
		else:
			gateway = features.get_gateway(arg_vals["gateway"])
			job = gateway.query_job(arg_vals["jobid"])
			print features.generate_feature(arg_vals["features"], job)


