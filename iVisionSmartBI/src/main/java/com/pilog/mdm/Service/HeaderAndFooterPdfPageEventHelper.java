package com.pilog.mdm.Service;

import com.itextpdf.text.Anchor;
import com.itextpdf.text.BadElementException;
import com.itextpdf.text.BaseColor;
import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Element;
import com.itextpdf.text.Font;
import com.itextpdf.text.Image;
import com.itextpdf.text.PageSize;
import com.itextpdf.text.Phrase;
import com.itextpdf.text.Rectangle;
import com.itextpdf.text.pdf.BaseFont;
import com.itextpdf.text.pdf.ColumnText;
import com.itextpdf.text.pdf.PdfPageEventHelper;
import com.itextpdf.text.pdf.PdfWriter;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletRequest;

import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

/**
 *
 * @author Onkar
 */

public class HeaderAndFooterPdfPageEventHelper extends PdfPageEventHelper {

    private final ResourceLoader resourceLoader;

    public HeaderAndFooterPdfPageEventHelper(ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
    }

//    HttpServletRequest request;
//
//   HeaderAndFooterPdfPageEventHelper(HttpServletRequest request, PdfWriter pdfWriter, Document document) {
//        try {
//            this.request = request;
//            String user = (String) request.getSession(false).getAttribute("ssUsername");
//            System.out.println("user::" + user);
//            DateTimeFormatter dateTimeFormater = DateTimeFormatter.ofPattern("dd/MM/yyyy");
//            LocalDateTime now = LocalDateTime.now();
//            Font ffont = new Font(Font.FontFamily.HELVETICA, 20, Font.BOLD);
//            Font ffont2 = new Font(Font.FontFamily.HELVETICA, 10, Font.NORMAL);
//            Phrase headerCreateDate = new Phrase(dateTimeFormater.format(now), ffont2);
//            Phrase headerMail = new Phrase("Mail : info@piloggroup.com" , ffont2);
//            Phrase headerPhone = new Phrase("Phone : +91 630 176 0928" , ffont2);
//            System.out.println("onStartPage() method > Writing header in file");
//            Rectangle rect = pdfWriter.getBoxSize("rectangle");
//
//		// TOP LEFT
//            ColumnText.showTextAligned(pdfWriter.getDirectContent(),
//                    Element.ALIGN_JUSTIFIED, headerCreateDate, rect.getLeft(),
//                    790, 0);
//            ColumnText.showTextAligned(pdfWriter.getDirectContent(),
//                    Element.ALIGN_JUSTIFIED, headerMail, rect.getLeft(),
//                    810, 0);
////            ColumnText.showTextAligned(pdfWriter.getDirectContent(),
////                    Element.ALIGN_JUSTIFIED, headerPhone, rect.getLeft(),
////                    770, 0);
//
//		// TOP MEDIUM
//		//        ColumnText.showTextAligned(pdfWriter.getDirectContent(),
//		//                Element.ALIGN_CENTER, header, rect.getRight() / 2,
//		//                rect.getTop(), 0);
//		// TOP RIGHT
//		//            ColumnText.showTextAligned(pdfWriter.getDirectContent(),
//		//                    Element.ALIGN_CENTER, header, rect.getRight() / 2,
//		//                    rect.getTop(), 0);
//            String IMG = "C:\\Users\\PiLog\\images\\PiLog Logo.png";
//            Image image = Image.getInstance(IMG);
//            image.scalePercent(20);
//            float width = PageSize.A4.getWidth();
//            image.setAbsolutePosition(rect.getRight() - 50, rect.getTop());
//            pdfWriter.getDirectContent().addImage(image);
//        } catch (BadElementException ex) {
//            Logger.getLogger(HeaderAndFooterPdfPageEventHelper.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (IOException ex) {
//            Logger.getLogger(HeaderAndFooterPdfPageEventHelper.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (DocumentException ex) {
//            Logger.getLogger(HeaderAndFooterPdfPageEventHelper.class.getName()).log(Level.SEVERE, null, ex);
//        }
//
//    }
//
//    public void onStartPage(PdfWriter pdfWriter, Document document) {
//        try {
//            Rectangle borderRectangle = new Rectangle(30, 30, 559, 759);
//            borderRectangle.enableBorderSide(1);
//            borderRectangle.setBorder(Rectangle.BOX);
//            borderRectangle.setBorderWidth(2);
//            borderRectangle.setBorderColor(BaseColor.BLACK);
//            document.add(borderRectangle);
//            DateTimeFormatter dateTimeFormater = DateTimeFormatter.ofPattern("dd/MM/yyyy");
//            LocalDateTime now = LocalDateTime.now();
//
//            Font ffont = new Font(Font.FontFamily.HELVETICA, 20, Font.BOLD);
//            Font ffont2 = new Font(Font.FontFamily.HELVETICA, 10, Font.BOLD);
//            Phrase headerMail = new Phrase("Mail : info@piloggroup.com" , ffont2);
////            Phrase header = new Phrase("Published By PiLog", ffont);
//            Phrase headerCreateDate = new Phrase(dateTimeFormater.format(now), ffont2);
//            Phrase header1 = new Phrase("Created By : " + (String) request.getSession(false).getAttribute("ssUsername"), ffont2);
//            System.out.println("onStartPage() method > Writing header in file");
//            Rectangle rect = pdfWriter.getBoxSize("rectangle");
//
//            // TOP LEFT
//            ColumnText.showTextAligned(pdfWriter.getDirectContent(),
//                    Element.ALIGN_JUSTIFIED, headerCreateDate, rect.getLeft(),
//                    790, 0);
//            ColumnText.showTextAligned(pdfWriter.getDirectContent(),
//                    Element.ALIGN_JUSTIFIED, headerMail, rect.getLeft(),
//                    810, 0);
//
//// TOP MEDIUM
////        ColumnText.showTextAligned(pdfWriter.getDirectContent(),
////                Element.ALIGN_CENTER, header, rect.getRight() / 2,
////                rect.getTop(), 0);
//// TOP RIGHT
////            ColumnText.showTextAligned(pdfWriter.getDirectContent(),
////                    Element.ALIGN_CENTER, header, rect.getRight() / 2,
////                    rect.getTop(), 0);
//            String IMG = "C:\\Users\\PiLog\\images\\PiLog Logo.png";
//
//            Image image = Image.getInstance(IMG);
//            image.scalePercent(20);
//            float width = PageSize.A4.getWidth();
////            image.setAbsolutePosition((rect.getLeft() + rect.getRight()) / 2 + 220, rect.getTop());
//            image.setAbsolutePosition(rect.getRight() - 50, rect.getTop());
//            pdfWriter.getDirectContent().addImage(image);
//
//            Font FONT = new Font(Font.FontFamily.HELVETICA, 50, Font.BOLD, BaseColor.LIGHT_GRAY);
//                ColumnText.showTextAligned(pdfWriter.getDirectContentUnder(),
//                        Element.ALIGN_CENTER,
//                        new Phrase("                      PiLog Cloud  PiLog Cloud  PiLog Cloud      PiLog Cloud", FONT),
//                        320,
//                        420f,
//                        55);
//        } catch (BadElementException ex) {
//            Logger.getLogger(HeaderAndFooterPdfPageEventHelper.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (IOException ex) {
//            Logger.getLogger(HeaderAndFooterPdfPageEventHelper.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (DocumentException ex) {
//            Logger.getLogger(HeaderAndFooterPdfPageEventHelper.class.getName()).log(Level.SEVERE, null, ex);
//        }
//    }
//
//    public void onEndPage(PdfWriter pdfWriter, Document document) {
//
//        Font FONT = new Font(Font.FontFamily.HELVETICA, 52, Font.BOLD, new GrayColor(0.85f));
//
//        System.out.println("onEndPage() method > Writing footer in file");
//        Rectangle rect = pdfWriter.getBoxSize("rectangle");
//        // BOTTOM LEFT
//        ColumnText.showTextAligned(pdfWriter.getDirectContent(),
//                Element.ALIGN_CENTER, new Phrase(""),
//                rect.getLeft() + 15, rect.getBottom(), 0);
//
//        // BOTTOM MEDIUM
//        ColumnText.showTextAligned(pdfWriter.getDirectContent(),
//                Element.ALIGN_CENTER, new Phrase(""),
//                rect.getRight() / 2, rect.getBottom(), 0);
//
//        // BOTTOM RIGHT
//        ColumnText.showTextAligned(pdfWriter.getDirectContent(),
//                Element.ALIGN_CENTER, new Phrase(""),
//                rect.getRight() - 10, rect.getBottom(), 0);
//    }
HttpServletRequest request;

   public HeaderAndFooterPdfPageEventHelper(HttpServletRequest request, PdfWriter pdfWriter, Document document, ResourceLoader resourceLoader) {
       this.resourceLoader = resourceLoader;
       try {

            Resource textFontResource = this.resourceLoader.getResource("classpath:/static/fonts/NotoSansCJKjp-Regular.otf");
            File textFontFile = textFontResource.getFile();

            Resource pilogLogoResource = this.resourceLoader.getResource("classpath:/static/images/PiLog_Logo_New.png");
            File pilogLogoFile = pilogLogoResource.getFile();

            String textFontFilePath = textFontFile.getAbsolutePath();
            String pilogLogoImage = pilogLogoFile.getAbsolutePath();
            //String textFontFilePath = request.getServletContext().getRealPath("/fonts/NotoSansCJKjp-Regular.otf");
            BaseFont customBaseFont = null;
            customBaseFont = BaseFont.createFont(textFontFilePath, BaseFont.IDENTITY_H, BaseFont.NOT_EMBEDDED);
            this.request = request;
            String user = (String) request.getSession(false).getAttribute("ssUsername");
            System.out.println("user::" + user);
            DateTimeFormatter dateTimeFormater = DateTimeFormatter.ofPattern("dd/MM/yyyy");
            LocalDateTime now = LocalDateTime.now();
            Font ffont = new Font(Font.FontFamily.HELVETICA, 20, Font.BOLD);
            Font font = new Font(customBaseFont, 10, Font.NORMAL);
            Phrase headerCreateDate = new Phrase(dateTimeFormater.format(now), font);
            Phrase headerMail = new Phrase("Mail : info@piloggroup.com", font);
            Phrase headerPhone = new Phrase("Phone : +91 630 176 0928", font);
            System.out.println("onStartPage() method > Writing header in file");
            Rectangle rect = pdfWriter.getBoxSize("rectangle");

            // TOP LEFT
            ColumnText.showTextAligned(pdfWriter.getDirectContent(),
                    Element.ALIGN_JUSTIFIED, headerCreateDate, rect.getLeft(),
                    rect.getTop(), 0);
//            ColumnText.showTextAligned(pdfWriter.getDirectContent(),
//                    Element.ALIGN_JUSTIFIED, headerMail, rect.getLeft(),
//                    810, 0);
//            ColumnText.showTextAligned(pdfWriter.getDirectContent(),
//                    Element.ALIGN_JUSTIFIED, headerPhone, rect.getLeft(),
//                    770, 0);

            // TOP MEDIUM
            //        ColumnText.showTextAligned(pdfWriter.getDirectContent(),
            //                Element.ALIGN_CENTER, header, rect.getRight() / 2,
            //                rect.getTop(), 0);
            // TOP RIGHT
            //            ColumnText.showTextAligned(pdfWriter.getDirectContent(),
            //                    Element.ALIGN_CENTER, header, rect.getRight() / 2,
            //                    rect.getTop(), 0);
//            String IMG = "C:\\Users\\PiLog\\images\\PiLog Logo.png";
//            Image image = Image.getInstance(IMG);
            //header logo image
            //String pilogLogoImage = request.getServletContext().getRealPath("/images/PiLog_Logo_New.png");
            Image image = Image.getInstance(pilogLogoImage);
//            String logoImageString = request.getParameter("pilogLogoString");
//            Image image = getImage(logoImageString);

            image.scalePercent(20);
            float width = PageSize.A4.getWidth();
            image.setAbsolutePosition(rect.getRight() - 50, rect.getTop());
            pdfWriter.getDirectContent().addImage(image);
        } catch (BadElementException ex) {
            Logger.getLogger(HeaderAndFooterPdfPageEventHelper.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(HeaderAndFooterPdfPageEventHelper.class.getName()).log(Level.SEVERE, null, ex);
        } catch (DocumentException ex) {
            Logger.getLogger(HeaderAndFooterPdfPageEventHelper.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public void onStartPage(PdfWriter pdfWriter, Document document) {
        try {
            /*Rectangle borderRectangle = new Rectangle(30, 30, 559, 795);
            borderRectangle.enableBorderSide(1);
            borderRectangle.setBorder(Rectangle.BOX);
            borderRectangle.setBorderWidth(2);
            borderRectangle.setBorderColor(BaseColor.BLACK);
            document.add(borderRectangle); // for border*/

            //custom font
            Resource textFontResource = resourceLoader.getResource("classpath:/static/fonts/NotoSansCJKjp-Regular.otf");
            File textFontFile = textFontResource.getFile();

            String textFontFilePath = textFontFile.getAbsolutePath();
            BaseFont customBaseFont = null;
            customBaseFont = BaseFont.createFont(textFontFilePath, BaseFont.IDENTITY_H, BaseFont.NOT_EMBEDDED);

            DateTimeFormatter dateTimeFormater = DateTimeFormatter.ofPattern("dd/MM/yyyy");
            LocalDateTime now = LocalDateTime.now();

            Font ffont = new Font(Font.FontFamily.HELVETICA, 20, Font.NORMAL);
            Font font = new Font(customBaseFont, 10, Font.NORMAL);
//            Phrase headerMail = new Phrase("Mail : info@piloggroup.com" , ffont2);
//            Phrase header = new Phrase("Published By PiLog", ffont);
            Phrase headerCreateDate = new Phrase(dateTimeFormater.format(now), font);
            Phrase header1 = new Phrase("Created By : " + (String) request.getSession(false).getAttribute("ssUsername"), font);
            System.out.println("onStartPage() method > Writing header in file");
            Rectangle rect = pdfWriter.getBoxSize("rectangle");

            // TOP LEFT
            ColumnText.showTextAligned(pdfWriter.getDirectContent(),
                    Element.ALIGN_JUSTIFIED, headerCreateDate, rect.getLeft(),
                    rect.getTop(), 0);
//            ColumnText.showTextAligned(pdfWriter.getDirectContent(),
//                    Element.ALIGN_JUSTIFIED, headerMail, rect.getLeft(),
//                    810, 0);

// TOP MEDIUM
//        ColumnText.showTextAligned(pdfWriter.getDirectContent(),
//                Element.ALIGN_CENTER, header, rect.getRight() / 2,
//                rect.getTop(), 0);
// TOP RIGHT
//            ColumnText.showTextAligned(pdfWriter.getDirectContent(),
//                    Element.ALIGN_CENTER, header, rect.getRight() / 2,
//                    rect.getTop(), 0);
//
            Resource pilogLogoResource = resourceLoader.getResource("classpath:/static/images/PiLog_Logo_New.png");
            File pilogLogoFile = pilogLogoResource.getFile();
            String pilogLogoImage = pilogLogoFile.getAbsolutePath();
            Image image = Image.getInstance(pilogLogoImage);
//            String logoImageString = request.getParameter("pilogLogoString");
//            Image image = getImage(logoImageString);
            image.scalePercent(20);
//            image.scalePercent(20);
            float width = PageSize.A4.getWidth();
//            image.setAbsolutePosition((rect.getLeft() + rect.getRight()) / 2 + 220, rect.getTop());
            image.setAbsolutePosition(rect.getRight() - 50, rect.getTop());
            pdfWriter.getDirectContent().addImage(image);

            Font FONT = new Font(customBaseFont, 60, Font.BOLD, BaseColor.LIGHT_GRAY);//td changed 60
            ColumnText.showTextAligned(pdfWriter.getDirectContentUnder(),
                    Element.ALIGN_CENTER,
                    new Phrase("PiLog Cloud     PiLog Cloud", FONT),
                    //                        new Phrase("PiLog Cloud  PiLog Cloud  PiLog Cloud", FONT),
                    320,
                    390f,
                    55);
        } catch (BadElementException ex) {
            Logger.getLogger(HeaderAndFooterPdfPageEventHelper.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(HeaderAndFooterPdfPageEventHelper.class.getName()).log(Level.SEVERE, null, ex);
        } catch (DocumentException ex) {
            Logger.getLogger(HeaderAndFooterPdfPageEventHelper.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void onEndPage(PdfWriter pdfWriter, Document document) {

        try {
            //custom font

            Resource textFontResource = resourceLoader.getResource("classpath:/static/fonts/NotoSansCJKjp-Regular.otf");
            File textFontFile = textFontResource.getFile();

            String textFontFilePath = textFontFile.getAbsolutePath();
            BaseFont customBaseFont = null;
            customBaseFont = BaseFont.createFont(textFontFilePath, BaseFont.IDENTITY_H, BaseFont.NOT_EMBEDDED);
            System.out.println("onEndPage() method > Writing footer in file");
            Rectangle rect = pdfWriter.getBoxSize("rectangle");
            // BOTTOM LEFT
            ColumnText.showTextAligned(pdfWriter.getDirectContent(),
                    Element.ALIGN_CENTER, new Phrase("info@piloggroup.com", new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK)),
                    rect.getLeft() + 57, rect.getBottom() - 11, 0);

            // BOTTOM MEDIUM
            ColumnText.showTextAligned(pdfWriter.getDirectContent(),
                    Element.ALIGN_CENTER, new Phrase(""),
                    rect.getRight() / 2, rect.getBottom(), 0);
            Anchor anchor = new Anchor("Copyright © 2021 PiLog Group", new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK));
            anchor.setReference("https://www.piloggroup.com/");

            // BOTTOM RIGHT
            ColumnText.showTextAligned(pdfWriter.getDirectContent(),
                    Element.ALIGN_CENTER, new Phrase("Copyright © 2021 PiLog Group", new Font(customBaseFont, 12, Font.NORMAL, BaseColor.BLACK)),
                    rect.getRight() - 70, rect.getBottom() - 11, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

