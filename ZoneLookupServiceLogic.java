package com.comcast.snp.netiq.utilities.maintenance.service;

import com.comcast.snp.netiq.utilities.maintenance.common.APIStatus;
import com.comcast.snp.netiq.utilities.maintenance.common.SubnetSchema;
import com.comcast.snp.netiq.utilities.maintenance.datasources.invdb.model.ExceptionIPsEntity;
import com.comcast.snp.netiq.utilities.maintenance.datasources.invdb.model.ZoneMappingEntity;
import com.comcast.snp.netiq.utilities.maintenance.common.ZoneResult;
import com.comcast.snp.netiq.utilities.maintenance.datasources.invdb.repository.ExceptionIPsRepository;
import com.comcast.snp.netiq.utilities.maintenance.datasources.invdb.repository.ZoneMappingRepository;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service("deviceServ")
public class ZoneLookupServiceImpl implements ZoneLookupService{
    public static final Logger log = LoggerFactory.getLogger(DeviceScheduleServiceImpl.class);

    @Autowired
    private ZoneMappingRepository repository;

    @Autowired
    private ExceptionIPsRepository exceptionRepository;

    @Value("${regex.ip}")
    protected String ipRegex;

    Pattern ipPattern = null;

    LoadingCache<String, Map<String, List>> zoneMappingCache;
    Integer zoneMappingCacheReloadHours = 24;
    LoadingCache<String, Map<String, List>> exceptionsCache;
    Integer exceptionsCacheReloadSeconds = 60;
    LoadingCache<String, Map<String, List>> aggregateCache;
    Integer aggregateCacheReloadSeconds = exceptionsCacheReloadSeconds;

    /**
     * initializes all the caches to identify subnet/exceptions
     */
    @PostConstruct
    public void init() {
        zoneMappingCache = CacheBuilder.newBuilder().maximumSize(1).refreshAfterWrite(zoneMappingCacheReloadHours, TimeUnit.HOURS).build(
                new CacheLoader<String, Map<String, List>>() {
                    @Override
                    public Map<String, List> load(String key) throws Exception {
                        //for any key given, it will return the hash map containing zoneMapping
                        Map<String, List> zoneInfoZoneMapping = new HashMap<String, List>();
                        //add all colored zones, and exceptions
                        List<ZoneMappingEntity> allDevices = new ArrayList<>();
                        allDevices.addAll(repository.findByOldZone("BLUE"));
                        allDevices.addAll(repository.findByOldZone("RED"));
                        allDevices.addAll(repository.findByOldZone("GREEN"));
                        allDevices.addAll(repository.findByOldZone("BLACK"));
                        allDevices.addAll(repository.findByOldZone("WHITE"));
                        allDevices.addAll(repository.findByOldZone("N/A"));
                        for (ZoneMappingEntity d: allDevices) addDeviceToZoneInfo(zoneInfoZoneMapping, d);
                        return zoneInfoZoneMapping;
                    }
                }

        );

        exceptionsCache = CacheBuilder.newBuilder().maximumSize(1).refreshAfterWrite(exceptionsCacheReloadSeconds, TimeUnit.SECONDS).build(
                new CacheLoader<String, Map<String, List>>() {
                    @Override
                    public Map<String, List> load(String key) throws Exception {
                        //for any String key given, will return all exceptions
                        Map<String, List> zoneInfoExceptions = new HashMap<String, List>();
                        List<ExceptionIPsEntity> allExceptions = new ArrayList<>();
                        allExceptions.addAll(exceptionRepository.findByService("EXCEPTION"));
                        for (ExceptionIPsEntity e: allExceptions) addExceptionToZoneInfo(zoneInfoExceptions, e);
                        return zoneInfoExceptions;
                    }
                }
        );

        aggregateCache = CacheBuilder.newBuilder().maximumSize(1).refreshAfterWrite(aggregateCacheReloadSeconds, TimeUnit.SECONDS).build(
                new CacheLoader<String, Map<String, List>>() {
                    @Override
                    public Map<String, List> load(String key) throws Exception {
                        Map<String, List> zoneMappingMap = zoneMappingCache.get("key");
                        Map<String, List> exceptionsMap = exceptionsCache.get("key");

                        Iterator zoneMappingIter = zoneMappingMap.entrySet().iterator();
                        Iterator exceptionsIter = exceptionsMap.entrySet().iterator();

                        Map<String,List> zoneInfoAggregate =  new HashMap<String, List>();

                        while(zoneMappingIter.hasNext()){
                            Map.Entry current = (Map.Entry) zoneMappingIter.next();

                            String subnet = current.getKey().toString();
                            if(!exceptionsMap.containsKey(subnet)) zoneInfoAggregate.put(subnet,(List)current.getValue());
                        }

                        while(exceptionsIter.hasNext()){
                            Map.Entry current = (Map.Entry) exceptionsIter.next();
                            zoneInfoAggregate.put(current.getKey().toString(),(List)current.getValue());
                        }

                        return zoneInfoAggregate;

                    }
                }
        );

        log.info("Zone Lookup Service caches built");
    }


    /**
     * addDeviceToZoneInfo will add subnet, info (CIDR, service, oldZone, newZone) to zoneInfo
     * @param zoneInfo map containing all possible subnets
     * @param d device to add
     */
    private void addDeviceToZoneInfo (Map zoneInfo, Object d){
        ZoneMappingEntity o = (ZoneMappingEntity) d;
        List<String> subnetList = new ArrayList<>();
        subnetList.add(o.getCidr());
        subnetList.add(o.getService());
        subnetList.add(o.getOldZone());
        subnetList.add(o.getNewZone());
        zoneInfo.put(o.getSubnet(),subnetList);
    }

    /**
     * addExceptionToZoneInfo will add subnet, info (CIDR, service, oldZone, newZone) to zoneInfo
     * @param zoneInfo map containing all possible subnets
     * @param e exception to add
     */
    private void addExceptionToZoneInfo (Map zoneInfo, Object e){
        List<String> subnetList = new ArrayList<>();
        ExceptionIPsEntity o = (ExceptionIPsEntity) e;
        subnetList.add(o.getCidr());
        subnetList.add(o.getService());
        subnetList.add(o.getOldZone());
        subnetList.add(o.getNewZone());
        zoneInfo.put(o.getSubnet(),subnetList);
    }

    /**
     * getAllDevices() returns all subnets with info (CIDR, service, zones)
     * @return Map of subnet, info pairs
     */
    @Override
    public ResponseEntity<APIStatus> getAllDevices() {
        Map<String, List> allMap;
        List<SubnetSchema> output = new ArrayList<>();
        try {
            allMap = aggregateCache.get("key");
        } catch (ExecutionException e){
            return null;
        }

        Iterator all = allMap.entrySet().iterator();
        while(all.hasNext()) {
            Map.Entry current = (Map.Entry) all.next();
            List val = (List) current.getValue();
            output = addSS(output, current.getKey().toString(), val.get(0).toString(),
                    val.get(1).toString(), val.get(2).toString(), val.get(3).toString());
        }
        Map<String,Object> response = new HashMap<>();
        response.put("response",output);
        return buildResponseEntity(new APIStatus(HttpStatus.OK, "All subnets", response));

    }


    /**
     * getSubnet
     * @param subnet subnet query and get info about
     * @return information about queried subnet
     */
    @Override
    public ResponseEntity<APIStatus> getSubnet(String subnet) {


        List val = new ArrayList();
        List<SubnetSchema> output = new ArrayList<>();
        Map<String,Object> response = new HashMap<>();

        try {
            val = aggregateCache.get("key").get(subnet);
        } catch (Exception e){
            log.warn("Subnet not found");
        }
        if (Objects.equals(null,val)) return buildResponseEntity(new APIStatus(HttpStatus.BAD_REQUEST, "Subnet not found", response));

        output = addSS(output,subnet,val.get(0).toString(),
                val.get(1).toString(),val.get(2).toString(),val.get(3).toString());
        response.put("response",output);
        return buildResponseEntity(new APIStatus(HttpStatus.OK, "Subnet found", response));
    }


    /**
     * toOctet will take in a binary bit string and ensure that it is returned with 8 total bits
     * @param number bit string of some length
     * @return number of length 8
     */
    private String toOctet (String number){
        if (number.length()==8) return number;
        while(number.length() != 8) number = '0' + number;
        return number;
    }

    /**
     * toValue converts the binary octet value to decimal
     * @param index the index of the octet to convert
     * @param binVal the octet to convert to decimal
     * @return an integer decimal value for binVal up to index
     */
    private int toValue (int index, String binVal){
        if (index==-1) index = 7;
        binVal = binVal.substring(0,index+1);
        while (binVal.length()!=8) binVal += '0';
        return Integer.parseInt(binVal,2);
    }

    /**
     * updateBest updates the best match (passed in) with new info
     * @param best List containing subnet, CIDR< service, oldZone, newZone info
     * @param subnet String
     * @param CIDR String
     * @param service String
     * @param oldZone String
     * @param newZone String
     * @return best newly updated
     */
    private List<String> updateBest (List<String> best, String subnet, String CIDR, String service, String oldZone, String newZone) {
        best.clear();
        best.add(subnet);
        best.add(CIDR);
        best.add(service);
        best.add(oldZone);
        best.add(newZone);

        return best;
    }

    /**
     * parse the IP/subnet and the value to convert to binary
     * @param IP address
     * @param octetToCheck integer of octet to check
     * @return List containing the split IP/subnet and the number to convert to binary
     */
    private List<String> parse (String IP, int octetToCheck) {
        List<String> output = new ArrayList<>();

        String ipSplit = "";
        String decToBin = "";
        int dots = 0;
        for (int i=0; i<IP.length();i++){
            if (dots == octetToCheck){
                break;
            }
            if (IP.charAt(i)=='.'){
                ipSplit += '.';
                dots++;
            } else {
                ipSplit += IP.charAt(i);
            }
        }

        //get the octet in question for IP
        int dots2 = 0;
        for (int j =0; j<IP.length(); j++){
            if (dots2 == octetToCheck){
                decToBin += IP.charAt(j);
            } else if (dots2 == octetToCheck+1){
                break;
            }
            if (IP.charAt(j)=='.'){
                dots2++;
            }
        }

        output.add(ipSplit);
        output.add(decToBin);
        return output;

    }

    /**
     * performCheck for a particular octet for the given IP and all possible subnets
     * @param IP user-inputted IP
     * @param zoneInfo all possible subnets
     * @param octet the octet performing the check on
     * @return the best match
     */
    private List<String> performCheck (String IP, Map<String,List> zoneInfo, int octet){
        List<String> fewerOctets = new ArrayList<>();

        //break up IP into chunks, added to fewerOctets
        String ipChunk = "";
        int dotCount = 0;
        for (int i=0;i<IP.length();i++){
            if (dotCount == octet){ //stop when added the sufficient number of octets to fewerOctets
                break;
            }
            if (IP.charAt(i)=='.'){
                dotCount++;
                fewerOctets.add(ipChunk);
                ipChunk = "";
            } else {
                ipChunk += IP.charAt(i);
            }
        }


        //to rejoin abbreviated chunks into string (with trailing .)
        String fewerOctetsStr = "";
        for (String chunk: fewerOctets) fewerOctetsStr += chunk + '.';


        //develop HashMap of <subnets, subnetInfo> that start with fewerOctetsStr
        Map<String, List> matches = new HashMap<String,List>();
        Iterator zoneInfoIter = zoneInfo.entrySet().iterator();
        while (zoneInfoIter.hasNext()){
            Map.Entry current = (Map.Entry)zoneInfoIter.next();
            if (current.getKey().toString().startsWith(fewerOctetsStr)){
                matches.put(current.getKey().toString(),(List)current.getValue());
            }
        }


        //test each match to see if the subnet/CIDR/IP combination all mesh
        List<String> best = new ArrayList<>();
        Iterator matchesIter = matches.entrySet().iterator();
        while(matchesIter.hasNext()){
            Map.Entry current = (Map.Entry) matchesIter.next();

            String subnet = current.getKey().toString();
            List<String> val = (List<String>)current.getValue(); // CIDR, service, oldZone, newZone
            int CIDR = Integer.parseInt(val.get(0));

            String service = val.get(1);
            String oldZone = val.get(2);
            String newZone = val.get(3);

            //determine the octet to check based on the CIDR
            int octetToCheck = -1; //negative int to detect error
            if(CIDR <=8){
                octetToCheck = 0;
            } else if (CIDR <=16){
                octetToCheck = 1;
            } else if (CIDR <=24){
                octetToCheck = 2;
            } else {
                octetToCheck = 3;
            }

            List<String> output1 = parse(IP,octetToCheck);
            List<String> output2 = parse(subnet,octetToCheck);

            String ipSplit = output1.get(0);
            String decToBin = output1.get(1);

            String currentSplit = output2.get(0);
            String currentVal = output2.get(1);

            if (Objects.equals(ipSplit, currentSplit)){ //if IP and CURRENT match up to the octet in question

                if (decToBin.charAt(decToBin.length()-1)=='.'){ //remove . if it's at the end of decToBin (IP) so that it can be parsed
                    decToBin = decToBin.substring(0,decToBin.length()-1);
                }

                int dectoBinInt = Integer.parseInt(decToBin); //convert decToBin (IP) to int

                String binOctet = toOctet(Integer.toBinaryString(dectoBinInt)); // convert decToBin (IP) to binary bit string octet

                int index = (CIDR%8)-1; //determine index to derive value from binOctet


                if (currentVal.charAt(currentVal.length()-1)=='.'){//remove . if it's at the end of currentVal (CURRENT) so that it can be parsed
                    currentVal = currentVal.substring(0,currentVal.length()-1);
                }
                int currentValInt = Integer.parseInt(currentVal); //convert currentValInt (CURRENT) to int
                int octetsVal = toValue(index, binOctet); //get value to compare octetsVal (IP)

                if (currentValInt == octetsVal){ // CURRENT v IP
                    if (best.isEmpty()){
                        best = updateBest(best,subnet,Integer.toString(CIDR),service,oldZone,newZone);
                    } else{
                        int bestCIDR = Integer.parseInt(best.get(1));
                        if (CIDR>bestCIDR){
                            best = updateBest(best,subnet,Integer.toString(CIDR),service, oldZone, newZone);
                        }
                    }
                }
            }
        }
        return best;
    }

    /**
     * setZoneResult formats zoneResult such that a JSON can be returned with IP, subnet, CIDR, service, oldZone, newZone info
     * @param r the zoneResult object that will be returned
     * @param IP the IP address that the user passed in
     * @param subnet the subnet that houses IP
     * @param CIDR the CIDR that corresponds to subnet
     * @param service the service that corresponds to subnet
     * @param oldZone the oldZone (color) that corresponds to subnet
     * @param newZone the newZone that corresponds to subnet
     * @return r that has been populated with IP, subnet, CIDR, service, oldZone, newZone
     */
    private ZoneResult setZoneResult(ZoneResult r, String IP, String subnet, String CIDR, String service, String oldZone, String newZone){
        r.setIP(IP);
        r.setSubnet(subnet);
        r.setCIDR(CIDR);
        r.setService(service);
        r.setOldZone(oldZone);
        r.setNewZone(newZone);
        return r;
    }


    /**
     * findZone attempts to find the zone corresponding to the user-inputted IP by removing octets
     * @param IP the user-inputted IP address
     * @return r (a zoneResult) in JSON form containing IP, subnet, CIDR, service, oldZone, and newZone information
     */
    private ZoneResult findZone (String IP){
        Map<String, List> zoneInfo;
        try {
            zoneInfo = aggregateCache.get("key");
        } catch (ExecutionException e){
            return null;
        }

        String affirmative = "A matching subnet was found for the supplied IP";

        ZoneResult r = new ZoneResult();
        if (zoneInfo.containsKey(IP)){
            log.info("IP was a complete match with a subnet");
            return setZoneResult(r,IP,IP,zoneInfo.get(IP).get(0).toString(),zoneInfo.get(IP).get(1).toString(),zoneInfo.get(IP).get(2).toString(),zoneInfo.get(IP).get(3).toString());
        }

        List<String> threeOctetBest = performCheck(IP, zoneInfo, 3);
        if (!threeOctetBest.isEmpty()){
            log.info(affirmative);
            return setZoneResult(r, IP, threeOctetBest.get(0),threeOctetBest.get(1),threeOctetBest.get(2),threeOctetBest.get(3),threeOctetBest.get(4));
        }

        List<String> twoOctetBest = performCheck(IP, zoneInfo, 2);
        if (!twoOctetBest.isEmpty()){
            log.info(affirmative);
            return setZoneResult(r, IP, twoOctetBest.get(0),twoOctetBest.get(1),twoOctetBest.get(2),twoOctetBest.get(3),twoOctetBest.get(4));
        }

        List<String> oneOctetBest = performCheck(IP, zoneInfo, 1);
        if (!oneOctetBest.isEmpty()){
            log.info(affirmative);
            return setZoneResult(r, IP, oneOctetBest.get(0),oneOctetBest.get(1),oneOctetBest.get(2),oneOctetBest.get(3),oneOctetBest.get(4));
        }
        log.info("No matching subnet was found for the supplied IP");
        return setZoneResult(r, IP, "UNKNOWN","UNKNOWN","UNKNOWN","UNKNOWN","UNKNOWN");


    }

    /**
     * validIP determines if an IP address is valid
     * @param IP the IP address that needs to be checked
     * @return trueIP String that removes all non digit nor . characters; returns blank if fewer than 3 .s
     */
    private String validIP(String IP){
        int dotCount = 0;
        String trueIP = "";
        for (int c=0;c<IP.length();c++){
            char current = IP.charAt(c);
            if(current=='.') dotCount++;
            if(Character.isDigit(current) || current=='.') trueIP += current;

        }

        if (dotCount != 3) trueIP = "";
        return trueIP;
    }

    protected Pattern getIpPattern(){
        if(ipPattern == null)
            ipPattern = Pattern.compile(ipRegex);
        return ipPattern;
    }

    /**
     * ZoneLookup is called when you have an IP and you want to return info pertaining to the subnet that houses IP
     * @param IP String
     * @return ResponseEntity<APIStatus> with subnet and corresponding info
     */
    @Override
    public ResponseEntity<APIStatus>  ZoneLookup(String IP) {
        log.info("IP received, about to process for subnet zone information");
        String trueIP = validIP(IP);
        Map<String,Object> response = new HashMap<>();
        if (trueIP.isEmpty()) {
            log.info("No IP received");
            return buildResponseEntity(new APIStatus(HttpStatus.BAD_REQUEST, "Empty IP",response));
        }
        Matcher matcher = getIpPattern().matcher(IP);
        if (matcher.matches()){
            log.info("Valid IP received");
            response.put("response",findZone(trueIP));
            return  buildResponseEntity(new APIStatus(HttpStatus.OK, "Zone lookup complete",response));
        }
        else{
            log.info("Invalid IP received");
            return buildResponseEntity(new APIStatus(HttpStatus.BAD_REQUEST, "Invalid IP",response));
        }
    }


    /**
     * IPinSubnet returns a boolean depending on if the given IP resides in the given subnet/CIDR
     * @param IP String
     * @param subnet String
     * @param CIDR String
     * @return true or false embedded in ResponseEntity<APIStatus>
     */
    @Override
    public ResponseEntity<APIStatus> IPinSubnet (String IP, String subnet, String CIDR){
        Matcher IPmatcher = getIpPattern().matcher(IP);
        Matcher subnetmatcher = getIpPattern().matcher(subnet);
        Map<String,Object> response = new HashMap<>();
        if (!IPmatcher.matches()) return buildResponseEntity(new APIStatus(HttpStatus.BAD_REQUEST, "Invalid IP",response));
        if (!subnetmatcher.matches()) return buildResponseEntity(new APIStatus(HttpStatus.BAD_REQUEST, "Invalid subnet",response));

        if (Objects.equals(IP, subnet)) {
            log.info("IP is the subnet");
            response.put("response",true);
            return buildResponseEntity(new APIStatus(HttpStatus.OK, "IP is the Subnet",response));
        }

        Map<String, List> zoneInfo = new HashMap<String, List>(); //only entry in zoneInfo is the subnet provided by user
        List<String> subnetList = new ArrayList<>();
        subnetList.add(CIDR);
        subnetList.add("SERVICE");
        subnetList.add("OLD ZONE");
        subnetList.add("NEW ZONE");
        zoneInfo.put(subnet,subnetList);

        String affirmative = "IP is in subnet";

        List<String> threeOctetBest = performCheck(IP, zoneInfo, 3);
        if (!threeOctetBest.isEmpty()) {
            log.info(affirmative);
            response.put("response",true);
            return buildResponseEntity(new APIStatus(HttpStatus.OK, affirmative,response));
        }

        List<String> twoOctetBest = performCheck(IP, zoneInfo, 2);
        if (!twoOctetBest.isEmpty()) {
            log.info(affirmative);
            response.put("response",true);
            return buildResponseEntity(new APIStatus(HttpStatus.OK, affirmative,response));
        }

        List<String> oneOctetBest = performCheck(IP, zoneInfo, 1);
        if (!oneOctetBest.isEmpty()) {
            log.info(affirmative);
            response.put("response",true);
            return buildResponseEntity(new APIStatus(HttpStatus.OK, affirmative,response));
        }
        String not = "IP is NOT in provided subnet";
        log.info(not);
        response.put("response",false);
        return buildResponseEntity(new APIStatus(HttpStatus.OK, not,response));
    }

    @Autowired
    private ExceptionIPsRepository exceptionIPsRepository;

    /**
     * addException adds new exceptions to maintenance.ExceptionIPs
     * @param subnet String
     * @param CIDR String
     * @param oldZone String
     * @param newZone String
     * @return returns String stating that the exception has been added
     */
    @Override
    @Transactional
    public ResponseEntity<APIStatus> addException(String subnet, String CIDR, String oldZone, String newZone) {
        Map<String,Object> response = new HashMap<>();
        ExceptionIPsEntity newException = new ExceptionIPsEntity(subnet,CIDR,"EXCEPTION",oldZone,newZone);
        exceptionRepository.save(newException);
        String responseStr = subnet+"/"+CIDR+" ("+oldZone+", "+newZone+")"+" was added to maintenance.ExceptionIPs";
        log.info(responseStr);
        response.put("response",newException);
        return buildResponseEntity(new APIStatus(HttpStatus.OK, "Exception Added ", response));
    }

    /**
     * addSS adds a SubnetSchema to output list
     * @param output list to add SubnetSchema to
     * @param subnet string
     * @param cidr string
     * @param service string
     * @param zoneOld string
     * @param zoneNew string
     * @return output list with newest SubnetSchema added
     */
    @Override
    public List<SubnetSchema> addSS (List<SubnetSchema> output, String subnet, String cidr, String service, String zoneOld, String zoneNew){
        SubnetSchema ss = new SubnetSchema();
        ss.setSubnet(subnet);
        ss.setCIDR(cidr);
        ss.setService(service);
        ss.setZoneOld(zoneOld);
        ss.setZoneNew(zoneNew);
        output.add(ss);
        return output;
    }

    /**
    * buildResponseEntity constructs an API response entity
    * @param apiStatus response apiStatus
    * @return ResponseEntity<APIStatus> constructed
    */
    private ResponseEntity<APIStatus> buildResponseEntity(APIStatus apiStatus) {
        log.info(apiStatus.toString());
        return new ResponseEntity<>(apiStatus, apiStatus.getStatus());
    }


}
