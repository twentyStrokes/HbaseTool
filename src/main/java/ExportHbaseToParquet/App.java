package ExportHbaseToParquet;

public class App {
  public static void main(String[] args) throws Exception {
    System.out.println("Version: 0.0.5");
    if (args.length == 0) {
      System.out.println("commends:");
    }
    String command = args[0];

    String[] subArgs = new String[args.length - 1];
    System.arraycopy(args, 1, subArgs, 0, args.length - 1);

    if ("ExportTxLog".equals(command)){
      ExportTxLog.main(subArgs);
    } else if ("ExportUrmtpinf".equals(command)){
      ExportUrmtpinf.main(subArgs);
    } else if ("ExportPwmtqpag".equals(command)){
      ExportPwmtqpag.main(subArgs);
    }else {
      System.out.println("Involve command:" + command);
    }
  }
}
