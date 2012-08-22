#!/usr/bin/env perl
use strict;
use warnings;
use feature ':5.10.0';
use lib 'lib';
use AnyEvent;
use Data::Dumper;
use Cmd::HiveRunner;

my $query = <<'END_HQL';
SELECT g_vw.ds,
    g_vw.locale,
    round(((SUM(
      CASE
        WHEN g_vw. au_first_pageview_ts >= pg_vw. last_pageview_ts  THEN 1
        ELSE 0
      END
    ) / COUNT(*)) * 100.0), 3) as bounce_rate
FROM vw_discoverau_general g_vw
JOIN (
  SELECT fp.ds,
      fp.locale,
      fp.session_id,
      MAX( fp.request_finished ) as last_pageview_ts
  FROM f_pageviews_hist fp
  WHERE fp.ds ='2012-07-31'
  GROUP BY fp.ds, fp.locale, fp.session_id
  ) pg_vw
ON (
  g_vw.ds = '2012-07-31' AND g_vw.ds= pg_vw.ds AND
  g_vw.locale=pg_vw.locale AND g_vw.session_id = pg_vw.session_id )
GROUP BY g_vw.ds, g_vw.locale
LIMIT 10
;
END_HQL

#$query = q{select * from dual where c = 'c'};

# set up a hive-runner
my $hive = Cmd::HiveRunner->new( user => 'sscaffidi', conf => { foo => 'bar' } );

# run the query, using the parameters provided in both the runner
# and in this method-call
my $task = $hive->run(
  conf  => { 'mapred.job.queue.name' => 'warehouseteam' },
  query => $query,
);

# spit out some status info every 10 seconds until the task is done
my $t; $t = AnyEvent->timer(after => 1, interval=> 10, cb => sub {
    undef $t if $task->state eq 'finished';
    say Dumper($task->jobs_info, $task->progress_info);
});
$task->wait;
say "DONE";
