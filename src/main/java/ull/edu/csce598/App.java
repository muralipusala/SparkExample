package ull.edu.csce598;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        WordCount wc = new WordCount();
        wc.run("resources/input_text.txt");
    }
}
