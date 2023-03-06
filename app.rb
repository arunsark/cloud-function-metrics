require "functions_framework"
require "json"
require "google/cloud/bigquery"

FunctionsFramework.http "metrics" do |request|
  input = JSON.parse request.body.read rescue {}
  bigquery = Google::Cloud::Bigquery.new project: "nomadic-basis-379613"
  dataset = bigquery.dataset "metrics", skip_lookup: true

  if ( input["table"].nil? )
    msg = "No table given"
  elsif ( input["operation"].nil? )
    msg = "No operation given"
  elsif ( input["data"].nil? || input["data"].length == 0 )
    msg = "No data given"
  end


  unless msg.nil?
    return msg
  end


  table = dataset.table input["table"]
  rows = []
  input["data"].each { |r| 
    r["created_at"] = Time.now.strftime('%Y-%m-%dT%H:%M:%S')
    rows.push(r) 
  }  
  insert_response = table.insert rows
  msg = "Inserted #{insert_response.insert_count()} Success #{insert_response.success?}"
end
