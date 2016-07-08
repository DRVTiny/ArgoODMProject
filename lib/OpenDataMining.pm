package OpenDataMining;
use utf8;
use v5.14.1;
use Data::Dumper qw(Dumper);
use Redis;
use LWP::Simple;
use Mojo::Base 'Mojolicious';
use JSON::XS qw(encode_json decode_json);
use POSIX qw(strftime);
use HTML::Entities;
use lib '/opt/libs/Perl5';
use CSV::Lite qw(csvPrint);
use Math::BigFloat ':constant';
use Math::Round qw(nearest);
use Date::Simple qw(date today);
use AnyEvent::HTTP qw(http_get);
use Mojolicious::Types;
use IO::Compress::Gzip 'gzip';
use IO::Uncompress::Gunzip 'gunzip';
my $types = Mojolicious::Types->new;
$types->type('csv' => 'text/csv', 'json'=>'application/json; charset=utf-8');
#$app->types->type('csv' => 'text/csv');

my %Methods=(
  'testua'=>{
    'cache'=>undef,
  },
  'time'=>{
    'cache'=>undef,
  }, # <- time
  'curxe'=>{
    'cache'=>{
      'enabled'=>1,
      'period'=>600,
      'key'=>'hshCurXe',
    },
    'source'=>{
      'url'=>'https://query.yahooapis.com/v1/public/yql?q=select+*+from+yahoo.finance.xchange+where+pair+=+%22USDRUB,EURRUB%22&format=json&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys&callback=',
    },
  }, # <- getcur
  'mostemp'=>{
    'cache'=>{
      'period'=>300,
      'key'=>'MosTemp',
    },
    'source'=>{
      'url'=>'http://rp5.ru/tmaps/sql.php',
    },
    'meteostations'=>{
      'key_name2id'=>'MeteoStationName2ID',
      'key_id2name'=>'MeteoStationID2Name',
      'list'=>[
        'Moscow_Balchug',
        'Moscow_VDNH',
        'Moscow_Reutov',
        'Moscow_Lyubercy',
        'Mosobl_Dzerjinskiy',
        'Moscow_Sheremetevo',
        'Moscow_MGU',
        'Moscow_Yug',  
      ],
    },
  }, # <- mostemp
  'euronews'=>{
    'cache'=>{
      'period'=>600,
      'key'=>'jsonEuroNews',
    },
    'source'=>{
      'url'=>'http://feeds.feedburner.com/euronews/ru/home/',
    },    
  }, # <- euronews
  'status'=>{},
  'mosforeca'=>{
    'format'=>'csv',
    'cache'=>{
      'enabled'=>0,
      'period'=>10800,
      'key'=>'MosForeca',
    },
    'source'=>{
      'url'=>'http://api.openweathermap.org/data/2.5/forecast?id=524901&APPID=77d32ed412018a59fdb538acf04d0273',
    },
  }, # <- mosforeca
  'mosrides'=>{
    'cache'=>{
      'enabled'=>0,
      'period'=>600,
      'key'=>'MosRides',
    },
    'compression'=>{
      'enabled'=>1,
      'type'=>'gzip',
    },
    'source'=>{
      'url'=>'http://m.katushkin.ru/rides',
    },
    'limit'=>{'max_days'=>31},
  } # <- mosrides
);

# This method will run once at server start
sub startup {
  my $self = shift;
  my $nowAtInit=time();
  my $jsonData;
  my $flStopProcessing=0;
  my $log=$self->app->log;
  $self->app->ua->proxy->detect();
  $log->debug('Detected HTTP proxy: '.$self->app->ua->proxy->http) if $self->app->ua->proxy->http;
  # Documentation browser under "/perldoc"
  # $self->plugin('PODRenderer');
  $self->secrets(['Cr4bUtH@cK']);
  $self->types->type('csv' => 'text/csv'); 
  # Router
  my $r = $self->routes;
#  (my $nnname = $self->app->req->url) =~ s%^/([^?]+)(?:\?.+)?$%$1%;
#  $log->debug('Name='.$nnname);
  # Normal route to controller
  $self->hook('before_dispatch' => sub {
      my $c = shift;
      $flStopProcessing=0;
      (my $slfName = $c->req->url) =~ s%^/([^?]+)(?:\?.+)?$%$1%;
      my $met=$Methods{$slfName};
      unless (defined($met)) {
        $c->render('json'=>{'error'=>'No such method'});
        return 0;
      }
      $log->debug('In before_dispatch()');
      return 1 unless $met->{'cache'} and $met->{'cache'}{'enabled'};
      $log->debug('Method '.$slfName.' require caching');
      my $cache=$met->{'cache'};
      my $ks=Redis->new;
      # If cache was not initialized yet -> return
      my $ksMyHash=$cache->{'key'};
      unless ($ks->hkeys($ksMyHash)) {
       $log->debug('No hash "'.$ksMyHash.'" found in Redis');
       return 1;
      }
      my $cacheTime=$cache->{'period'};
      if ($cacheTime>0) {
        my $tsLastValGet=$ks->hget($ksMyHash,'store_ts');
        if (abs(time()-$tsLastValGet)>$cacheTime) {
         $log->debug('Cache period '.$cacheTime.' sec. already expired, we have to get actual data');
         return 1;
        }
      } elsif ($cacheTime==-1 && (my $pass=$c->param('cachepass')) && (my $cacheSecret=$ks->hget($ksMyHash,'store_pwd'))) {
        if ( crypt($pass,'AAA') eq $cacheSecret ) {
          $log->debug('Cache modification accepted');
          return 1;
        } else {
          $flStopProcessing=1;
          $log->debug('Cache modification denied');
          $c->render('json'=>{"error"=>"Cache modification denied: wrong secret specified"},'status'=>403, 'format'=>'json');
#          $c->finish;
          return 0;
        }
      }
      $log->debug(join(' ','Method',$slfName,($cacheTime>0?'can':'MUST'),'be resolved by cache'));
      $flStopProcessing=1;
      my $val=$ks->hget($cache->{'key'},'store_val');
      utf8::decode($val);
      $c->render(
        'text'=>$val,
        'format'=>$met->{'format'} // 'json',
      );
  });
  $self->hook('after_dispatch' => sub {
      my $c = shift;
#      $log->debug('AFTER_DISPATCH CALLED');
      return 1 if $flStopProcessing;
      (my $slfName = $c->req->url) =~ s%^/([^?]+)(?:\?.+)?$%$1%;
      my $met=$Methods{$slfName};
      
      do { $log->debug('Caching not required'); return 1; } unless $met->{'cache'} and $met->{'cache'}{'enabled'};
      unless ( $jsonData ) {
        $log->debug($slfName.' require cahing, but there are no data to store in the cache provided!');
        return 0;
      }
      my $cache=$met->{'cache'};
      $log->debug("OK, lets try to store data in the cache");
      my $ks=Redis->new;
      $ks->hset($cache->{'key'},'store_val',$jsonData);
      $ks->hset($cache->{'key'},'store_ts',time());
      $log->debug('Data was saved here: '.$cache->{'key'}.'{"store_val"}');
  });
  $self->hook('after_render' => sub {
      my ($c, $output, $format) = @_;
      (my $slfName = $c->req->url) =~ s%^/([^?]+)(?:\?.+)?$%$1%;
      my $met=$Methods{$slfName};
            
      # Check if "gzip => 1" has been set in the stash
      do { $log->debug('Compression not required'); return 1 }
        unless $c->stash->{'gzip'} || (ref($met->{'compression'}) eq 'HASH' && $met->{'compression'}{'enabled'});

      # Check if user agent accepts GZip compression
      return unless ($c->req->headers->accept_encoding // '') =~ /gzip/i;
      $c->res->headers->append(Vary => 'Accept-Encoding');

      # Compress content with GZip
      $c->res->headers->content_encoding('gzip');
      gzip $output, \my $compressed;
      $$output = $compressed;
  
  });
  $r->get('/hayabusa' => sub {
    my $v=shift;
    my $log=$v->app->log;
    $log->debug('Request: ',$v->req->url);
    $v->render('json'=>['a','b','c']);
  });
  
  $r->get('/testua' => sub {
    my $v=shift;
#    $v->app->ua->proxy->http('http://msk1-vm-proxy01.unix.nspk.ru:3128');
# encode_json([map {utf8::decode($_); $v->app->log->debug('header h1: '.$_); $_} @{$dom->find('h1')->map('text')->to_array}]),
    my $dom=$v->app->ua->get('http://os2.ru')->res->body;
    $v->render(
      'text' => $dom,
      'format' => 'html'      
    );
  });
  
  $r->get('/time' => sub {
    my $v=shift;
    $v->render('text' => join('','{"time":',time(),'}'),'format'=>'json');
  });

  $r->get('/curxe' => sub {
    my $v=shift; 
    (my $slfName = $v->req->url) =~ s%^/([^?]+)(?:\?.+)?$%$1%;
    my $ua = LWP::UserAgent->new;
    
    my $curxe = decode_json($ua->get($Methods{$slfName}{'source'}{'url'})->decoded_content);
#    $log->debug('Get currency exchange rates: '.Dumper($curxe));
    $jsonData=encode_json {map {$_->{'Name'}=>$_->{'Rate'}} @{$curxe->{'query'}{'results'}{'rate'}}};
    $v->render('text' => $jsonData, 'format' => 'json');
  });

  $r->get('/mostemp' => sub {
    my $v=shift;
    (my $slfName = $v->req->url) =~ s%^/([^?]+)(?:\?.+)?$%$1%;
    my $ua=LWP::UserAgent->new();
    my $config=$Methods{$slfName};
    my $ks=Redis->new;
    my (%hshName2ID,%hshID2Name);
    
    my $keyName2ID=$config->{'meteostations'}{'key_name2id'};
    foreach my $msName (@{$config->{'meteostations'}{'list'}}) {
     my $msID=$ks->hget($keyName2ID,$msName);
     $hshName2ID{$msName}=$msID;
     $hshID2Name{$msID}=$msName;
    }
    my $srcUrl=$config->{'source'}{'url'};
    my $tmp=['data'=>'true',map { ('ids[]',$_) } keys %hshID2Name];
    $log->debug(Dumper $tmp);
    my $res=$ua->post($srcUrl,$tmp);
    if ($res->is_success) {
     $log->debug('Got temperatures in Moscow: '.$res->decoded_content);
     my $jsonTemp=decode_json($res->decoded_content);
     $jsonData=encode_json({map { $hshID2Name{$_}=>$jsonTemp->{$_} } keys $jsonTemp});
     $v->render('text'=>$jsonData,'format'=>'json', 'gzip'=>1);
     return 1;
    } else {
     $v->render('json'=>{'error'=>"Cant POST to $srcUrl"});
     return 0;
    }
    
  });
  
  $r->get('/mosforeca' => sub {
    my $v=shift;
    (my $slfName = $v->req->url) =~ s%^/([^?]+)(?:\?.+)?$%$1%;
    my $ua=LWP::UserAgent->new();
    my $config=$Methods{$slfName};

    my $fc=decode_json($ua->get($config->{'source'}{'url'})->decoded_content);
    my @csv=(
     ['Time','Temp','Humidity','Weather cond','Wind speed','Pressure'],
    );
    foreach my $fc3 ( @{$fc->{'list'}} ) {
      push @csv,[strftime('%Y-%m-%d %H:%M:%S',localtime($fc3->{'dt'})),
                 nearest(0.1,$fc3->{'main'}{'temp'}-273.150).'C',
                 $fc3->{'main'}{'humidity'}.'%',
                 $fc3->{'weather'}[0]{'description'},
                 $fc3->{'wind'}{'speed'}.'m/s',
                 nearest(0.01,$fc3->{'main'}{'pressure'}*0.1).'kPa',
                ];   
    }
    my @out;
#    $v->types->type('csv' => 'text/csv');
    csvPrint(\@out,\@csv);
    $v->render('text'=>join("\n",@out),'format'=>'csv','gzip'=>1);
  });
  
  $r->get('/mosrides' => sub {
    my $v=shift;
    (my $slfName = $v->req->url) =~ s%^/([^?]+)(?:\?.+)?$%$1%;
    my $config=$Methods{$slfName};
        
    my %hshRides;
    my $iSkip=0;
    my ($baseUrl,$srcUrl)=($config->{'source'}{'url'},undef);
    my ($siteUrl)=$baseUrl=~m%^\s*(https?://[^/]+)%i;
    my $maxDate=today()+$config->{'limit'}{'max_days'};
    my $ua=$v->app->ua;
    my @cmn;
    my $doExtractRides=sub {
      my ($refHtmlRides, $nSkip)=@_;
      $nSkip||=0;
      printf "In doExtractRides()\[%d\], htmlRides.length=%d\n", $nSkip, length($$refHtmlRides);
      utf8::decode($$refHtmlRides);
      while ($$refHtmlRides=~m/<a href="(\/rides\/(\d{4}\/\d{2}\/\d{2})\/\d+)"\s*>\s*([^<]+)\s*<\/a>.*?(\d+:\d+)[^<]*?<\/span>.+?<a\s+href="([^"]+)">Участвуют<\/a>.*?<span[^>]*>\s*(\d+)\s*<\/span>/sg) {
        my ($relUrl,$YYYYMMDD,$rideTitle,$HHMM,$urlParts,$nParts)=($1,$2,$3,$4,$5,$6);        
        $YYYYMMDD=~tr/\//-/;
#        last RIDEPAGE if $YYYYMMDD gt $maxDate;
        my $link=$siteUrl.$relUrl;
        say '['.$nSkip.'] =====> '.$link;
        next if exists $hshRides{$link};
        $hshRides{$link} = {
                      'day'=>      $YYYYMMDD,
                      'time'=>     $HHMM,
                      'fullTime'=> $YYYYMMDD.' '.$HHMM,
                      'title'=>    decode_entities($rideTitle),
                      'link'=>     $link,
                      'parts' => { 
                        'link' 	=> $urlParts=~m@^https?://@i?$urlParts:$siteUrl.$urlParts,
                        'n'	=> $nParts,
                      },
        };
      }
    };
    my $renderRides=sub {
      my $hr={'katushkin_rides'=>[ sort { $a->{'fullTime'} cmp $b->{'fullTime'} } values %hshRides ]};
      $jsonData=JSON::XS->new->utf8->encode($hr);
      $v->render('json'=>$hr);
    };
    # Get first page in usual "blocking" mode:
    my $htmlRides=$ua->max_redirects(5)->get($baseUrl)->res->body;
    open F,'>','/tmp/rides.html';
    print F $htmlRides;
    close(F);
    my @skip=sort { $a <=> $b } keys {map { $_=>1 } $htmlRides=~m%\?skip=(\d+)"%sg};
    $doExtractRides->(\$htmlRides);
    $log->debug('Ride pages: '.join(','=>@skip));
    my $nSkip;
    unless ($v->param('native')) {
      my $aeCondVar=AnyEvent->condvar;
      for my $ridesPageUrl (map $baseUrl.'/?skip='.$_, grep {$_<=45} @skip) {
        $log->debug('Trying to fetch URL (with AnyEvent): '.$ridesPageUrl); 
        $aeCondVar->begin;
        http_get $ridesPageUrl, 'headers'=>{'Accept-Encoding'=>'gzip, deflate'}, sub {
          my ($body,$headers)=@_;
          
          unless (substr($headers->{'Status'},0,1) eq '2') {
            $log->debug(sprintf 'Error: cant fetch URL <<%s>>. Status code: %d', $ridesPageUrl, $headers->{'Status'});
            $aeCondVar->end;
            return undef;
          } 
          
          my $flHTMLZiped=exists($headers->{'Content-Encoding'}) and $headers->{'Content-Encoding'}=~m/gzip/i;
          $log->debug('Data succesfully retrieved from: '.$ridesPageUrl.($flHTMLZiped?'. Content was compressed.':''));          
          my $refHtml=$flHTMLZiped?do {
                                    gunzip \$body => \my $uncomp;
                                    \$uncomp
                                   }
                                  :\$body;          
          if ($nSkip=($ridesPageUrl=~m%skip=(\d+)%)[0]) {
            push @cmn, $nSkip;
            my $tmp="/tmp/rides__${nSkip}.html";
            open my $fh,'>',$tmp;
            say $fh $$refHtml;
            close $fh;
            $log->debug("HTML was saved to the file: $tmp");
          }
          $doExtractRides->($refHtml,$nSkip);
          $aeCondVar->end;
        };
      }
      $aeCondVar->recv;
      $log->debug(Dumper \@cmn);
      $renderRides->();
    } else {
       $v->render_later;
      my $delay = Mojo::IOLoop->delay(sub {
        $log->debug('Delay finished!');
        $log->debug(Dumper \@cmn);
        $renderRides->();
      });
      for my $ridesPageUrl (map $baseUrl.'/?skip='.$_, grep {$_<=45} @skip) {
        $log->debug('Trying to fetch URL: '.$ridesPageUrl);
        $ua->cookie_jar->ignore(sub {1});
        my $end=$delay->begin;
        $ua->max_redirects(5)->get($ridesPageUrl, sub {
          my ($userAgent,$rqResult)=@_;
          $log->debug("Yeah, my url is $ridesPageUrl");
          my $html=$rqResult->res->body;
          if ($nSkip=($ridesPageUrl=~m%skip=(\d+)%)[0]) {
            push @cmn, $nSkip;
            my $tmp="/tmp/rides__${nSkip}.html";
            open my $fh,'>',$tmp;
            say $fh $html;
            close $fh;
            $log->debug("HTML was saved to the file: $tmp");
          }
          $doExtractRides->(\$html,$nSkip);
          $end->();
        });
      }
#      Mojo::IOLoop->start unless Mojo::IOLoop->is_running;
    }
  }); # <- /mosrides
  
  $r->get('/status' => sub {
    my $v=shift;
    $v->render('json' => 
      { 'version'=>'pre 0.001',
        'status'=>'API is not consistent (changed very frequently) and not intended to any usage except of debugging by its developers',
        'next_release'=>{
          'date'=>'2015/10/01',
          'target_version'=>'0.01',
        },
        'params'=>$v->req->params->to_hash,        
        'methods'=>[keys %Methods],
        'env'=>\%ENV,
      });
  }); # <- /status
  
  $r->get('/euronews' => sub {
    my $v=shift;
    my $ua = LWP::UserAgent->new;
    $ua->env_proxy;
  #  my $rss = $v->app->ua->get('http://feeds.feedburner.com/euronews/ru/home/')->res->body;
    my $rss = $ua->get('http://feeds.feedburner.com/euronews/ru/home/')->decoded_content;
  #  return $v->render('text'=>$rss, format=>'txt');
    my @news = $rss =~ m%<title>(.+?)</title>%sg;
  #  return $v->render('text'=>'>>'.join("\n",@news).'<<', format=>'txt');
    $v->render('text'=>
                join('','{euronews:[',join(',', map { s%"%\\"%g; '"'.$_.'"' } grep { $_ ne 'euronews' } @news),']}'),
               'format'=>'json',
              );
  });  
}

1;
