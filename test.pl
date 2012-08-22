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
    say Dumper($task->jobs_info, $task->progress_info);
    undef $t if $task->state eq 'done';
});


$task->wait;
#print Dumper $task;

__END__

# on init:
$job->cmd;          # /usr/bin/sudo -E -u sscaffid /usr/local/hive/bin/hive -hiveconf foo=bar -e 'select * from t_foo where foo != "yarr";'
$job->start_time;   # 1234556788
$job->run_time;     # 0
$job->state;        # init

# as it starts:
$job->history_file; # /tmp/sscaffid/hive_job_log_sscaffid_201208211744_614380010.txt
$job->id;           # job_201208202150_0002
$job->tracking_url; # http://localhost:50030/jobdetails.jsp?jobid=job_201208202150_0002
$job->kill_cmd;     # /usr/local/hadoop/bin/hadoop job  -Dmapred.job.tracker=localhost:8021 -kill job_201208202150_0002
$job->state;        # start

# as it runs
$job->progress;     # { map => 80, reduce => 33 }
$job->run_time;     # 15
$job->state;        # run

# when it is done
$job->run_time;     # 30
$job->end_time;     # 1234556818
$job->state;        # done

# if anything goes wrong, perhaps it should throw an exception?
$job->errors;       # not sure, but it seems like it needs this, right?

