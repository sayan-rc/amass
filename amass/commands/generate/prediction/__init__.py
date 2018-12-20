import amass
import amass.predict
from amass.commands import CommonArgs, Arg, Opt
import re

class Command(amass.commands.Command):
	usage = CommonArgs(
		"""
		Run predictions.
		""",
		[
			Arg("features", "Name of cached feature set to run prediction on"),
			Arg("gateway", "Name of cached gateway features")
		],
		[
			Opt("filtererrors", "Only run training and prediction on selected job errors", ""),
			Opt("split", "Split the features into a train/test split", "0.6")
		]
	)

	def __init__(self):
		amass.commands.Command.__init__(self)
		self.file = __file__

	def run(self, config, args):
		arg_vals = self.parse_args(args)
		self.logger.info("Running prediction")

		features = amass.features.Features(config)

		filter_errors = None
		if arg_vals["filtererrors"] != "":
			filter_errors = re.split("\s*,\s*", arg_vals["filtererrors"])

		features, results = features.get_features_results(
			arg_vals["features"], arg_vals["gateway"], filter_errors)
		if len(results) < 1:
			amass.abort("Unable to find matching features to run prediction")
		split_percentage = float(arg_vals["split"])
		split_index = int(split_percentage * len(features))
		train_features = features[:split_index]
		test_features = features[split_index:]
		train_results = results[:split_index]
		test_results = results[split_index:]
		predict = amass.predict.Prediction()
		print predict.train(train_features, train_results)



