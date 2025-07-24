﻿using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Application.Shared.Models;

public class Workspace
{
    [Key]
    [MaxLength(10)]
    [Required(ErrorMessage = "Workspace Id is required")]
    [StringLength(10, ErrorMessage = "Workspace Id cannot exceed 10 characters")]
    public string? Id { get; set; }

    [Required(ErrorMessage = "Workspace name is required")]
    [StringLength(100, ErrorMessage = "Workspace name cannot exceed 100 characters")]
    public string? Name { get; set; }

    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public DateTime? CreatedOn { get; set; } = DateTime.Now;

    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public DateTime? ModifiedOn { get; set; } = DateTime.Now;


    public string? CreatedBy { get; set; }
    public string? ModifiedBy { get; set; }

    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public bool? IsDeleted { get; set; } = false;


    [NotMapped]
    public bool? IsSelected { get; set; }

}
