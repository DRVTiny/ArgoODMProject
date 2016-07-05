#!/usr/bin/perl
use strict;
use warnings;
use LWP::UserAgent;
use JSON qw(decode_json encode_json);
use Redis;
my $srcUrl='http://rp5.ru/tmaps/sql.php';
my %hshMetPoints=(
  'Moscow_Balchug'=>152492,
  'Moscow_VDNH'=>5483,
  'Moscow_Reutov'=>7081,
  'Moscow_Lyubercy'=>5056,
  'Mosobl_Dzerjinskiy'=>2863,
  'Moscow_Sheremetevo'=>9208,
  'Moscow_MGU'=>5491,
  'Moscow_Yug'=>5489,  
);
my $r=Redis->new;
while (my ($m,$id)=each %hshMetPoints) {
 $r->hset('MeteoStationName2ID',$m,$id);
 $r->hset('MeteoStationID2Name',$id,$m);
}
exit;
my %rhshMetPoints;
while ( my ($k,$v) = each %hshMetPoints ) { $rhshMetPoints{$v}=$k }

my $r=LWP::UserAgent->new();
my $res=$r->post($srcUrl,['data'=>'true',map { ('ids[]',$_) } values %hshMetPoints]);

if ($res->is_success) {
 my $jsonTemp=decode_json($res->decoded_content);
 print 'Content-Type:application/json',"\n\n",
        encode_json({map { $rhshMetPoints{$_}=>$jsonTemp->{$_} } keys $jsonTemp});
} else {
 print '{"error":"Cant POST to '.$srcUrl.'"}';
}
