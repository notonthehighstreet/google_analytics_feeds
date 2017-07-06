require "google/api_client"

# @api public
module GoogleAnalyticsFeeds
  # Raised if login fails.
  #
  # @api public
  class AuthenticationError < StandardError ; end
  
  # Raised if there is an HTTP-level problem retrieving reports.
  #
  # @api public
  class HttpError < StandardError ; end

  # @api private
  API_VERSION = 'v3'

  # A Google Analytics session, used to retrieve reports.
  # @api public
  class Session
    # @api private
    SCOPES = ['https://www.googleapis.com/auth/analytics.readonly'].freeze

    # Creates a new session.
    def initialize(project)
      @project = project
      @authorized = false
    end

    # Log in to Google Analytics using a service account and a key file
    #
    # This should be done before attempting to fetch any reports.
    def login(service_account, pem_key_file)
      return @client if @client

      key = Google::APIClient::KeyUtils.
        load_from_pem(pem_key_file, 'notasecret')

      auth = Signet::OAuth2::Client.
        new(:token_credential_uri => 'https://accounts.google.com/o/oauth2/token',
            :audience => 'https://accounts.google.com/o/oauth2/token',
            :scope => SCOPES.join(' '),
            :issuer => service_account,
            :signing_key => key)

      @client = Google::APIClient.new(:authorization => auth,
                                     :application_name => "ruby-google-analytics-feeds",
                                     :application_version => "1.1.0",
                                     :faraday_option => {:timeout => 360})
    end

    def authorize
      @client.authorization.fetch_access_token!
      @authorized = true
    end

    def discover_api
      @analytics = @client.discovered_api('analytics', API_VERSION)
    end

    # Retrieve a report from Google Analytics.
    #
    # Rows are yielded to a RowHandler, provided either as a class,
    # instance or a block.
    def fetch_report(report, handler=nil, &block)
      handler  = block if handler.nil?

      authorize unless @authorized
      discover_api unless @analytics

      response = report.retrieve(@client, @analytics)
      DataFeedParser.new(handler).parse_rows(response.data)
    end
  end

  # A SAX-style row handler.
  #
  # Extend this class and override the methods you care about to
  # handle data feed row data.
  #
  # @abstract
  # @api public
  class RowHandler
    # Called when each row is parsed.
    #
    # By default, does nothing.
    #
    # @param row Hash
    def row(row)
    end
  end
  
  # @api private
  module Naming
    # Returns a ruby-friendly symbol from a google analytics name.
    #
    # For example:
    #
    #     name_to_symbol("ga:visitorType") # => :visitor_type
    def name_to_symbol(name)
      name.sub(/^ga\:/,'').gsub(/(.)([A-Z])/,'\1_\2').downcase.to_sym
    end
    
    # Returns a google analytics name from a ruby symbol.
    #
    # For example:
    #
    #     symbol_to_name(:visitor_type) # => "ga:visitorType"
    def symbol_to_name(sym)
      parts = sym.to_s.split("_").map(&:capitalize)
      parts[0].downcase!
      "ga:" + parts.join('')
    end
  end

  # @api private
  class RowParser
    include Naming

    def initialize(header)
      @header = header.map { |h| name_to_symbol(h) }
    end

    def parse(row)
      Hash[row.map.with_index { |val, i| [@header[i].to_sym, val] }]
    end
  end

  # Construct filters for a DataFeed.
  # 
  # @api private
  class FilterBuilder
    include Naming

    def initialize
      @filters = []
    end

    def build(&block)
      instance_eval(&block)
      @filters.join(';')
    end

    # TODO: remove duplication

    def eql(name, value)
      filter(name, value, '==')
    end

    def not_eql(name, value)
      filter(name, value, '!=')
    end

    def contains(n, v)
      filter(n, v, '=@')
    end

    def not_contains(n, v)
      filter(n, v, '!@')
    end

    def gt(n, v)
      filter(n, v, '>')
    end

    def gte(n, v)
      filter(n, v, '>=')
    end

    def lt(n, v)
      filter(n, v, '<')
    end

    def lte(n, v)
      filter(n, v, '<=')
    end
        
    def match(n, v)
      filter(n, v, '=~')
    end

    def not_match(n, v)
      filter(n, v, '!~')
    end

    private

    def filter(name, value, operation)
      @filters << [symbol_to_name(name), operation, value.to_s].join('')
    end
  end

  # @api public
  class DataFeed
    include Naming
    
    def initialize
      @params = {}
    end

    # Sets the profile id from which this report should be based.
    #
    # @return [GoogleAnalyticsFeeds::DataFeed] a cloned DataFeed.
    def profile(id)
      clone_and_set {|params|
        params['ids'] = symbol_to_name(id)
      }
    end

    # Sets the metrics for a query.
    #
    # A query must have at least 1 metric for GA to consider it
    # valid. GA also imposes a maximum (as of writing 10 metrics) per
    # query.
    #
    # @param names [*Symbol] the ruby-style names of the dimensions.
    # @return [GoogleAnalyticsFeeds::DataFeed] a cloned DataFeed.
    def metrics(*vals)
      clone_and_set {|params|
        params['metrics'] = vals.map {|v| symbol_to_name(v) }.join(',')
      }
    end

    # Sets the dimensions for a query.
    #
    # A query doesn't have to have any dimensions; Google Analytics
    # limits you to 7 dimensions per-query at time of writing.
    #
    # @param names [*Symbol] the ruby-style names of the dimensions.
    # @return [GoogleAnalyticsFeeds::DataFeed] a cloned DataFeed.
    def dimensions(*names)
      clone_and_set {|params|
        params['dimensions'] = names.map {|v| symbol_to_name(v) }.join(',')
      }
    end

    # Sets the start and end date for retrieved results
    # @return [GoogleAnalyticsFeeds::DataFeed] a cloned DataFeed.
    def dates(start_date, end_date)
      clone_and_set {|params|
        params['start-date'] = start_date.strftime("%Y-%m-%d")
        params['end-date'] = end_date.strftime("%Y-%m-%d")
      }
    end

    # Sets the start index for retrieved results
    # @return [GoogleAnalyticsFeeds::DataFeed]
    def start_index(i)
      clone_and_set {|params|
        params['start-index'] = i.to_s
      }
    end

    # Sets the maximum number of results retrieved.
    #
    # Google Analytics has its own maximum as well.
    # @return [GoogleAnalyticsFeeds::DataFeed]
    def max_results(i)
      clone_and_set {|params|
        params['max-results'] = i.to_s
      }
    end

    # Filter the result set, based on the results of a block.
    #
    # All the block methods follow the form operator(name,
    # value). Supported operators include: eql, not_eql, lt, lte, gt,
    # gte, contains, not_contains, match and not_match - hopefully all
    # self-explainatory.
    #
    # Example:
    #
    #    query.
    #      filter {
    #        eql(:dimension, "value")
    #        gte(:metric, 3)
    #      }
    # 
    # @return [GoogleAnalyticsFeeds::DataFeed]
    def filters(&block)
      clone_and_set {|params|
        params['filters'] = FilterBuilder.new.build(&block)
      }
    end

    # Use a dynamic advanced segment.
    #
    # Block methods follow the same style as for filters. Named
    # advanced segments are not yet supported.
    # 
    # @return [GoogleAnalyticsFeeds::DataFeed]
    def segment(&block)
      clone_and_set {|params|
        params['segment'] = "dynamic::" + FilterBuilder.new.build(&block)
      }
    end

    # Sorts the result set by a column.
    #
    # Direction can be :asc or :desc.
    def sort(column, direction)
      clone_and_set {|params|
        c = symbol_to_name(column)
        params['sort'] = (direction == :desc ? "-#{c}" : c)
      }
    end

    # @api private
    def retrieve(client, analytics)
      client.execute!(
        :api_method => analytics.data.ga.get,
        :parameters => @params)
    end

    # @api private
    def clone
      obj = super
      obj.instance_variable_set(:@params, @params.clone)
      obj
    end

    protected

    attr_reader :params

    private

    def clone_and_set
      obj = clone
      yield obj.params
      obj
    end
  end

  # @api private
  class DataFeedParser
    def initialize(handler)
      @handler = handler
    end

    # Parse rows from a result object.
    def parse_rows(data)
      header = data.column_headers.map { |c| c.name.gsub("ga:","") }
      data.rows.map { |row| @handler.row(RowParser.new(header).parse(row)) }
    end
  end
end
