<?php
require_once 'vendor/autoload.php';

use Symfony\Component\Yaml\Yaml;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

$s = [];
$startTime = 0;
$queue = [];

function processRequest(int $id) {
	global $s, $startTime, $queue;
	$path = sprintf($s->rrd->file_path_fmt, $id);
	while (0 < count($queue[$id])) {
		$req = array_shift($queue[$id]);
		if (!file_exists($path)) {
			printf("Creating RRD file: %s\n", $path);
			rrd_create(
				$path,
				[
					'--start', $req->at - 1,
					'--step', $s->rrd->step,
					"DS:value1:GAUGE:" . $s->rrd->heartbeat . ":U:U",
					"DS:value2:GAUGE:" . $s->rrd->heartbeat . ":U:U",
					"DS:value3:GAUGE:" . $s->rrd->heartbeat . ":U:U",
					"RRA:AVERAGE:0.5:1:60",
				]
			);
		}
		$args = $req->values;
		array_unshift($args, $req->at);
		printf("Updating RRD file: %s @ %s\n", $path, $req->at);
		rrd_update($path, $args);
		$dt = microtime(true) - $startTime;
		printf("%f [sec]\n", $dt);
	}
	unset($queue[$id]);
}

function onAmqpMessage(AMQPMessage $msg) {
	global $startTime, $queue;
	if ($startTime == 0) $startTime = microtime(true);
	$req = json_decode($msg->body);
	printf("Received a message %s\n", json_encode($req));
	if (!isset($queue[$req->id])) {
		$queue[$req->id] = [$req];
		processRequest($req->id);
	}
	else {
		$queue[$req->id][] = $req;
	}
}

function main() {
	global $s;
	$yamlSrc = file_get_contents("settings.yml");
	$s = Yaml::parse($yamlSrc, Yaml::PARSE_OBJECT_FOR_MAP);

	printf(
		"Connecting to amqp://%s@%s:%d\n",
		$s->amqp->user,
		$s->amqp->host,
		$s->amqp->port ?: 5672
	);
	$conn = new AMQPStreamConnection(
		$s->amqp->host,
		$s->amqp->port ?: 5672,
		$s->amqp->user,
		$s->amqp->pass
	);
	$ch = $conn->channel();

	printf("Waiting for messages\n");
	$ch->basic_consume(
		$s->amqp->queue, //queue
		"",              //consumer_tag
		false,           //no_local
		false,           //no_ack
		false,           //exclusive
		false,           //nowait
		"onAmqpMessage"  //callback
	);

	while(count($ch->callbacks)) $ch->wait();
}

main();
