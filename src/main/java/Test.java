public class Test {
    public static void main(String[] args) {
        String testEntry = "1,2,,3,,4,5,";
        String[] entryLoaded = testEntry.split(",");
        System.out.println(entryLoaded.length);
    }
}
