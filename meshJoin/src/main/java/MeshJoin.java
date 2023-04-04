import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.checkerframework.checker.units.qual.A;

import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;

public class MeshJoin {
    private Connection sourceConnection=null;	//Connection variables
    private Connection destConnection=null;
    private final Integer tuple=50;				//tuples retrieved from transaction table
    private Integer TransactionsTotal=10000;
    private final Integer partitions=10;		//partition of diskbuffer
    private final Integer numTransactions=50;
    private String password;
    private String source;
    private String dest;
    private int rowsAdded=0;
    ListMultimap<String, Transaction> hash = ArrayListMultimap.create();
    //ListMultimap<String, Map> hashMap1 = ArrayListMultimap.create();
    
    
    
    ArrayList<Product> diskBuffer1= new ArrayList<>();
    ArrayList<Customer> diskBuffer2= new ArrayList<>();
    Queue<ArrayList<String>> meshJoinQueue = new LinkedList<ArrayList<String>>();

    MeshJoin() throws ClassNotFoundException, SQLException {
    	//building connection with databases
    	Scanner myObj = new Scanner(System.in);
    	System.out.println("Enter root");
    	String user = myObj.nextLine();
    	
    	Scanner myObj2 = new Scanner(System.in);
    	System.out.println("Enter password");
    	String pass = myObj.nextLine();
    	
    
    	
    	
    	
    	//user="root";
    	password=pass;
    	source="db";
    	dest="Metro_dwh";
        Class.forName("com.mysql.cj.jdbc.Driver");
        this.sourceConnection=DriverManager.getConnection("jdbc:mysql://localhost/"+source+"?"+"user="+user+"&password="+password+"&autoReconnect=true&useSSL=false");
        this.destConnection=DriverManager.getConnection("jdbc:mysql://localhost/"+dest+"?"+"user="+user+"&password="+password+"&autoReconnect=true&useSSL=false");
        
        //counting number of total rows in transactions table
        String sql=("Select count(*) from transactions");
        PreparedStatement pstmt = sourceConnection.prepareStatement(sql);
        ResultSet rs = pstmt.executeQuery();
        if (rs.next())
        	TransactionsTotal=rs.getInt(1);
        
        System.out.println(TransactionsTotal);
        	
    }

    public int retrieveTransaction(Integer id) throws SQLException {
    	//Fetching 50 rows from transaction table
        int latest=0;
        ArrayList<String> productIDs= new ArrayList<>();
        Statement st = sourceConnection.createStatement();
        String sql = ("SELECT * FROM transactions where TRANSACTION_ID > ? ORDER BY TRANSACTION_ID ASC LIMIT ?;");
        PreparedStatement pstmt = sourceConnection.prepareStatement(sql);
        pstmt.setInt(1, id);
        pstmt.setInt(2, numTransactions);
        //System.out.println(pstmt);
        ResultSet rs = pstmt.executeQuery();
        while(rs.next()) {
        	
        	//--------------------------
        	Map m1 = new Map();
        	//--------------------------
            latest=rs.getInt("TRANSACTION_ID");
            Transaction temp = new Transaction();
            temp.Transaction_ID=latest;
            temp.Product_ID=rs.getString("PRODUCT_ID");
            temp.Customer_ID=rs.getString("CUSTOMER_ID");
            //temp.Customer_Name=rs.getString("CUSTOMER_NAME");
            temp.Store_ID=rs.getString("STORE_ID");
            temp.Store_Name=rs.getString("STORE_NAME");
            temp.Time_ID = rs.getString("Time_ID");
            temp.T_Date=rs.getDate("T_DATE");
            temp.Quantity=rs.getInt("QUANTITY");

            productIDs.add(temp.Product_ID);
            

            //---------------------------------------
            
            //m1.custKey = temp.Product_ID;
            //m1.t = temp;
            //this.hashMap1.put(temp.Customer_ID, m1);
            
            //---------------------------------------
            
            this.hash.put(temp.Product_ID, temp);
        }
        meshJoinQueue.add(productIDs);
        System.out.println(latest);
        return latest;
    }

    public void retrieveMasterDate(Integer id) throws SQLException {
    	//Fetching one partition from masterdata
        this.diskBuffer1.clear();
        this.diskBuffer2.clear();
        Statement st = sourceConnection.createStatement();
        String sql = ("SELECT a.PRODUCT_ID, a.PRODUCT_NAME, a.SUPPLIER_ID, a.SUPPLIER_NAME, a.PRICE FROM\n" +
                "    (SELECT *, ROW_NUMBER() OVER ( ORDER BY PRODUCT_ID ) row_num FROM  products) as a\n" +
                "WHERE a.row_num>?\n" +
                "LIMIT 10;");
        PreparedStatement pstmt = sourceConnection.prepareStatement(sql);
        //System.out.println(pstmt);
        pstmt.setInt(1, id);
        ResultSet rs = pstmt.executeQuery();
        while(rs.next()) {
            Product temp = new Product();
            temp.PRODUCT_ID=rs.getString(1);
            temp.PRODUCT_NAME=rs.getString(2);
            temp.SUPPLIER_ID=rs.getString(3);
            temp.SUPPLIER_NAME=rs.getString(4);
            temp.PRICE=rs.getDouble(5);
            this.diskBuffer1.add(temp);
        }
        
        Statement st2 = sourceConnection.createStatement();
        String sql2 = ("SELECT a.CUSTOMER_ID, a.CUSTOMER_NAME FROM\n" +
                "    (SELECT *, ROW_NUMBER() OVER ( ORDER BY CUSTOMER_ID ) row_num FROM  customers) as a\n" +
                "WHERE a.row_num>?\n" +
                "LIMIT 10;");
        PreparedStatement pstmt2 = sourceConnection.prepareStatement(sql2);
        
        pstmt2.setInt(1, id);
        //System.out.println(pstmt2);
        ResultSet rs2 = pstmt2.executeQuery();
        System.out.println(pstmt2);
        while(rs2.next()) {
            Customer temp = new Customer();
            temp.customer_id=rs2.getString(1);
            temp.customer_name=rs2.getString(2);
            this.diskBuffer2.add(temp);
            //System.out.print("Size of my disk is = ");
            //System.out.println(this.diskBuffer2.size());
        }
        
        
        
    }

    public void loadSupplier(String id, String name) throws SQLException{
        String sql1=("INSERT INTO dim_supplier(supplier_id,supplier_name)\n" +
                "SELECT * FROM\n" +
                "    (SELECT ? as supplier_id, ? as supplier_name) as a\n" +
                "WHERE NOT exists(\n" +
                "        SELECT supplier_id FROM dim_supplier WHERE supplier_id = ?\n" +
                "    );");
        PreparedStatement pstmt = this.destConnection.prepareStatement(sql1);
        pstmt.setString(1, id);
        pstmt.setString(2, name);
        pstmt.setString(3, id);
        System.out.print(pstmt);
        pstmt.executeUpdate();
    }

    public void loadCustomer(String id, String name) throws SQLException{
    	
        String sql2=("INSERT INTO dim_customer(customer_id,customer_name)\n" +
                "SELECT * FROM\n" +
                "    (SELECT ? as customer_id, ? as customer_name) as a\n" +
                "WHERE NOT exists(\n" +
                "        SELECT customer_id FROM dim_customer WHERE customer_id = ?\n" +
                "    );");
        PreparedStatement pstmt=this.destConnection.prepareStatement(sql2);
        pstmt.setString(1, id);
        pstmt.setString(2, name);
        pstmt.setString(3, id);
        pstmt.executeUpdate();
        if(!name.equals("")) {
	        String msql = ("Update dim_customer set customer_name=? where customer_id=?");
	        PreparedStatement pstmt1=this.destConnection.prepareStatement(msql);
	        pstmt1.setString(1, name);
	        pstmt1.setString(2,id);
	        pstmt1.executeUpdate();
        }
        
    }

    public void loadStore(String id, String name) throws SQLException{
        String sql3=("INSERT INTO dim_store(store_id,store_name)\n" +
                "SELECT * FROM\n" +
                "    (SELECT ? as store_id, ? as store_name) as a\n" +
                "WHERE NOT exists(\n" +
                "        SELECT store_id FROM dim_store WHERE store_id = ?\n" +
                "    );");
        PreparedStatement pstmt=this.destConnection.prepareStatement(sql3);
        pstmt.setString(1, id);
        pstmt.setString(2, name);
        pstmt.setString(3, id);
        pstmt.executeUpdate();
    }

    public void loadProduct(String id, String name) throws SQLException{
        String sql4=("INSERT INTO dim_product(product_id,product_name)\n" +
                "SELECT * FROM\n" +
                "    (SELECT ? as product_id, ? as product_name) as a\n" +
                "WHERE NOT exists(\n" +
                "        SELECT product_id FROM dim_product WHERE product_id = ?\n" +
                "    );");
        PreparedStatement pstmt=this.destConnection.prepareStatement(sql4);
        pstmt.setString(1, id);
        pstmt.setString(2, name);
        pstmt.setString(3, id);
        pstmt.executeUpdate();

    }

    public void loadDate(Date date) throws SQLException {
        String sql5=("INSERT INTO dim_date(date,day, month, quarter, year)\n" +
                "SELECT * FROM\n" +
                "    (SELECT ? as date, ? as day, ? as month, ? as quarter, ? as year) as a\n" +
                "WHERE NOT exists(\n" +
                "        SELECT date FROM dim_date WHERE date = ?\n" +
                "    );");
        PreparedStatement pstmt=this.destConnection.prepareStatement(sql5);
        pstmt.setDate(1,  date);
        String day= new SimpleDateFormat("EEEE", Locale.ENGLISH).format(date);
        pstmt.setString(2, day);
        pstmt.setInt(3, date.getMonth()+1);
        int q;
        if (date.getMonth()+1 <=3)
            q=1;
        else if (date.getMonth()+1 <=6)
            q=2;
        else if (date.getMonth()+1 <=9)
            q=3;
        else
            q=4;
        pstmt.setInt(4, q);
        int year=date.getYear()+1900;
        pstmt.setInt(5, year);
        pstmt.setDate(6, date);
        pstmt.executeUpdate();
    }

    public void loadFact(int t_id, String pid, String cid, String sid, Date date, String suid, int q, double sale) throws SQLException{
    	System.out.print("tid id = ");
    	System.out.println(t_id);
        String sql6=("INSERT INTO transaction_fact value (?, ?, ?, ?, ?, ?, ?, ?);");
        PreparedStatement pstmt=this.destConnection.prepareStatement(sql6);
        pstmt.setInt(1, t_id);
        pstmt.setString(2, pid);
        pstmt.setString(3, cid);
        pstmt.setString(4, sid);
        pstmt.setDate(5, date);
        pstmt.setString(6,suid);
        pstmt.setInt(7,q);
        pstmt.setDouble(8,sale);
        try {
        pstmt.executeUpdate();
        System.out.println("********************Rows added: "+rowsAdded+"*******************");
        }
        catch(Exception e) {
        String msql = ("Update transaction_fact set product_id=? where transaction_id=?");
        PreparedStatement mpstmt=this.destConnection.prepareStatement(msql);
        mpstmt.setString(1, pid);
        mpstmt.setInt(2,t_id);
        	//System.out.print("");
        }
        
        
    }

    public void loadTuple(Transaction tuple) throws SQLException {
        //checking supplier dimension
        this.loadSupplier(tuple.Supplier_ID, tuple.Supplier_Name);

        //checking customer dimension
        this.loadCustomer(tuple.Customer_ID, tuple.customer_name);

        //checking store dimension
        this.loadStore(tuple.Store_ID, tuple.Store_Name);

        //checking product dimension
        this.loadProduct(tuple.Product_ID,tuple.Product_Name);

        //checking date dimension
        this.loadDate((Date) tuple.T_Date);

        //inserting into fact table
        this.loadFact(tuple.Transaction_ID,tuple.Product_ID,tuple.Customer_ID, tuple.Store_ID,(Date) tuple.T_Date, tuple.Supplier_ID, tuple.Quantity,tuple.Total_Sale) ;

    }

    public void updateQueue() throws SQLException{
        ArrayList<String> toRemove = this.meshJoinQueue.remove();

        for (String id : toRemove) {
            ArrayList<Transaction> toDelete = new ArrayList<>();
            Collection<Transaction> matched = this.hash.get(id);
            for (Transaction t : matched) {
                if (t.Product_ID != null) {
                    toDelete.add(t);
                }
            }
            this.hash.get(id).removeAll(toDelete);
        }
        System.out.print("new size");
        System.out.println(this.meshJoinQueue.size());

    }

    public void runMeshJoin() throws SQLException {
        int j=0;
        
        for (int i=0; i<this.TransactionsTotal; j+=10 ){   //Read tuples from transactions
            i=this.retrieveTransaction(i);

            
            if (j== this.tuple){	//  load masterdata in chunks
                j=0;
            }
            this.retrieveMasterDate(j);
            int buff2 = 0;
            // Find values from diskbuffer in hash map
            for (int x=0; x<10; x++){
            	System.out.print(x);
                Customer current = this.diskBuffer2.get(x);
                Product current2 = this.diskBuffer1.get(x);
                Product current3 =  null;
                /*if(buff2==19) {
                    current3 = this.diskBuffer1.get(buff2);
                    }
                    else {
                    	current3 = this.diskBuffer1.get(buff2+1);
                    }*/
                
                List<Transaction> hash_match= this.hash.get(current2.PRODUCT_ID);
                //List<Map>match1=this.hashMap1.get(current.PRODUCT_ID);
                if (!hash_match.isEmpty()){
                    //if row found enrich data
                    for (int y=0 ; y<hash_match.size(); y++){
                        
                    	
                    	hash_match.get(y).Product_Name= current2.PRODUCT_NAME;
                        hash_match.get(y).Supplier_ID= current2.SUPPLIER_ID;
                        hash_match.get(y).Supplier_Name= current2.SUPPLIER_NAME;
                        hash_match.get(y).Total_Sale=hash_match.get(y).Quantity*current2.PRICE;
                    	System.out.print("cust_id is");
                    	System.out.print(hash_match.get(y).Customer_ID);
                        System.out.println(current.customer_id);
                        if(hash_match.get(y).Customer_ID.equals(current.customer_id)) {
                        	System.out.println("=============================matchec==========================================");
                        	hash_match.get(y).customer_name = current.customer_name;
                            
                        	
                        }
                        else {
                        	
                        	hash_match.get(y).customer_name="";
                        	
                        }
                        this.loadTuple(hash_match.get(y));
                        /*else if(match.get(y).Product_ID.equals(current3.PRODUCT_ID)) {
                        	match.get(y).Product_Name= current3.PRODUCT_NAME;
                            match.get(y).Supplier_ID= current3.SUPPLIER_ID;
                            match.get(y).Supplier_Name= current3.SUPPLIER_NAME;
                            match.get(y).Total_Sale=match.get(y).Quantity*current3.PRICE;
                            this.loadTuple(match.get(y));
                        	this.loadTuple(match.get(y));
                        	
                        }*/
                        
                        
                        
                        
                        
                        // 5. Load tuple in DWH
                        
                    }
                }
                buff2 = buff2 + 2;
            }

            
            System.out.print("this is pointer");
            System.out.println(this.meshJoinQueue.size());
            if (this.meshJoinQueue.size()==this.partitions) {
                this.updateQueue();
            }
        }
        //do same for remaining queue
        for (int i=0; i<this.partitions-1; i++, j+=10)
        {
            if (j== this.tuple){
                j=0;
            }
            this.retrieveMasterDate(j);
            int buff2 = 0;
            for (int x=0; x<10; x++){
                Customer current = this.diskBuffer2.get(x);
                Product current2 = this.diskBuffer1.get(x);
                Product current3 =  null;
                /*if(buff2==19) {
                	current3 = this.diskBuffer1.get(buff2);
                }
                else {
                	current3 = this.diskBuffer1.get(buff2+1);
                }*/
                List<Transaction> hash_match= this.hash.get(current2.PRODUCT_ID);
                if (!hash_match.isEmpty()){
                	 for (int y=0 ; y<hash_match.size(); y++){
                    
                	
                	hash_match.get(y).Product_Name= current2.PRODUCT_NAME;
                    hash_match.get(y).Supplier_ID= current2.SUPPLIER_ID;
                    hash_match.get(y).Supplier_Name= current2.SUPPLIER_NAME;
                    hash_match.get(y).Total_Sale=hash_match.get(y).Quantity*current2.PRICE;
                	
                    
                    if(hash_match.get(y).Customer_ID.equals(current.customer_id)) {
                    	hash_match.get(y).customer_name = current.customer_name;
                    	
                        
                    }
                    else {
                    	
                    	hash_match.get(y).customer_name="";
                    	
                    }
                    
                    this.loadTuple(hash_match.get(y));
                        // 5. Load tuple in DWH
                        
                    }
                }
                buff2 = buff2 + 2;
            }
            //System.out.println("Queue Remaining size is : "+this.pointersQueue.size());
            this.updateQueue();

        }
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        MeshJoin meshJ = new MeshJoin();
        meshJ.runMeshJoin();
    }
}

