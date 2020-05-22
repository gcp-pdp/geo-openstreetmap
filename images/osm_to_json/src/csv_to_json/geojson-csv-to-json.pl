#!/usr/bin/perl
use strict;
use warnings;
use JSON;
use Text::CSV::Encoded;
use Encode qw( decode );


#geometry,osm_id,osm_way_id,osm_version,osm_timestamp,all_tags

my $csv = Text::CSV::Encoded->new({ sep_char => ',',escape_char => '"',encoding_in  => "iso-8859-1" });
my @field_names;
my $i = 1;
while (my $line = <>) {
  chomp $line;
  $line = decode( 'iso-8859-1', $line );
  if ( $i == 1 ) {
    @field_names = split /,/, $line;
  }
  else {
      if ($csv->parse($line)) {
      my @fields = $csv->fields();
      my $geometry = JSON::decode_json($fields[0]);
      my $osm_id = $fields[1];
      my $osm_way_id = $fields[2];
      my $osm_version = $fields[3];
      my $osm_timestamp = $fields[4];
      my $all_tags = $fields[5];
      $all_tags =~ s/""/\\"/g;
      $all_tags =~ s/\r/\\r/gs;
      $all_tags =~ s/\t/\\t/gs;
      $all_tags =~ s/\\\\/DOUBLEBACKSLASH/g;
      my @tags = ();
      while ( $all_tags =~ m/\G.*?"(.*?[^\\])"=>"(.*?[^\\])"(,|$)/g ) {
        my $k = $1;
        my $v = $2;
        if ( $v =~ m/\\"$/ ) { warn "MATCHED '\\'"; $v .= '"'; }
        $k =~ s/DOUBLEBACKSLASH/\\\\/;
        $v =~ s/DOUBLEBACKSLASH/\\\\/;
        #warn "$k\t=>\t$v";
        push @tags, {"key" => $k, "value" => $v};
      }
      my $json_tags = '[' . join(',', map { '{"key":"' . $_->{key} . '","value":"' . $_->{value} . '"}' } @tags) . ']';
      #$all_tags = '[' . join(",",(map{qq({"key":"$_","value":"$at{$_}"})} keys %at)) . ']';
      eval {
        $json_tags = JSON::encode_json(JSON::decode_json($json_tags));
      };
      if ( $@ ) {
        print STDERR "failed to JSON encode at line $i: $@, offending data:\n";
        print STDERR "\torig: $all_tags\n";
        print STDERR "\tjson: $json_tags\n";
      }
      else {
        my $genc = JSON::encode_json($geometry);
        $genc =~ s/"/\\"/g;
        print sprintf(qq({"geometry":"%s","osm_id":"%s","osm_way_id":"%s","osm_version":%d,"osm_timestamp":"%s","all_tags":%s}\n),
          $genc, $osm_id, $osm_way_id, $osm_version, $osm_timestamp, $json_tags
        );
      }
    }
    else {
      print STDERR "failed to parse $i:\t$line\n";
    }
  }
  $i++;
}
