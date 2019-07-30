public class Main {
	public static void main(final String[] args) {
		final String filename = args[0];

		final csvHandler csvHandler = new csvHandler();

		System.out.println("Loading " + filename);
		csvHandler.loadData(filename);
		csvHandler.saveData(filename);
		System.out.println("Saved file: " + filename + "_filtered.csv");
	}
}
