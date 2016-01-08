package test;

import com.airbnb.di.hive.replication.ExchangePartitionParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

public class ExchangePartitionParserTest {
    private static final Log LOG = LogFactory.getLog(
            ExchangePartitionParserTest.class);

    @Test
    public void testParse() {
        String query = "ALTER TABLE test_db.test_table_exchange_to EXCHANGE PARTITION(ds='1', hr='2') WITH TABLE test_db.test_table_exchange_from";
        String query2 = "alter table xp_flog.checkout_flow_event exchange " +
                "partition (d='2015-09-07') " +
                "with table flog.checkout_flow_event";
        String query3 = "ALTER TABLE tmp_brain.extrema_listing_decline_dates_unavailable_20120814 EXCHANGE PARTITION(ds='2012-08-14') WITH TABLE extrema.listing_decline_dates_unavailable";
        ExchangePartitionParser parser = new ExchangePartitionParser();
        boolean parsed = parser.parse(query);
        LOG.debug("parsed=" + parsed);
        LOG.debug("exchangeFromSpec=" + parser.getExchangeFromSpec());
        LOG.debug("exchangeToSpec=" + parser.getExchangeToSpec());
        LOG.debug("partitionName=" + parser.getPartitionName());
    }
}
