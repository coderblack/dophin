import org.lionsoul.ip2region.DataBlock;
import org.lionsoul.ip2region.DbConfig;
import org.lionsoul.ip2region.DbMakerConfigException;
import org.lionsoul.ip2region.DbSearcher;

public class Ip2RegionTest {

    public static void main(String[] args) throws Exception {
        String ip = "192.168.1.1";

        DbSearcher dbSearcher = new DbSearcher(new DbConfig(), "data/ip2region/ip2region.db");

        DataBlock result = dbSearcher.memorySearch(ip);

        String region = result.getRegion();
        System.out.println(region);


    }
}
